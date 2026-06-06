# Decisiones de arquitectura

Decisiones no obvias tomadas durante el desarrollo. Cada entrada explica el **qué**, el **por qué** y las **alternativas descartadas**.

---

## Por qué ARDEF e IAR no usan Step Functions

**Decisión:** `lmbd-vi-ardef` y `lmbd-mc-iar` se invocan directamente desde el router (async), sin pasar por Step Functions.

**Razón:** Son archivos de reglas y rangos de BINes — procesos relativamente livianos y autocontenidos. No requieren las múltiples etapas pesadas del flujo transaccional.

**Alternativa descartada:** Crear un Step Function propio para ARDEF/IAR. Se descartó porque añade complejidad operacional sin beneficio real dado el tamaño del procesamiento.

---

## Por qué Glue y no Lambda para Calculate e Interchange

**Decisión:** `glue-vi-calculate` y `glue-vi-interchange` son Glue jobs (PySpark), no Lambdas.

**Razón:** 
- La lógica de cálculo de fees requiere joins complejos y operaciones sobre millones de registros simultáneamente.
- PySpark en Glue permite procesamiento distribuido que no cabe en el modelo de memoria/timeout de Lambda (máx 10240 MB / 900s).
- El job de interchange contrasta la tarificación propia contra los registros VSS (Data Quality) — operación que requiere tener ambos conjuntos de datos en memoria al mismo tiempo.

**Glue config:** Calculate: G.1X × 2 workers. Interchange: G.2X × 4 workers. Glue 4.0.

---

## Por qué el diseño es configuration-driven (DynamoDB)

**Decisión:** La lógica de campos, validaciones y patrones de archivo vive en DynamoDB, no hardcodeada en el código.

**Razón:** Visa y Mastercard actualizan sus especificaciones periódicamente. Tener la definición de campos en DynamoDB permite ajustar sin redesplegar Lambdas.

**Tablas involucradas:**
- `itx-file-pattern` → qué tipo de archivo es cada uno (regex por prioridad)
- `itx-visa-fields` → definición de campos por tipo de registro (~430 items)
- `itx-client` → configuración por cliente (encoding MC, etc.)

---

## Por qué chunked processing en Lambdas

**Decisión:** Las Lambdas de procesamiento (transform, extract, clean) dividen los archivos en chunks en vez de cargarlos completos en memoria.

**Razón:** Los archivos interchange pueden superar 1.5 GB. Cargarlos completos en memoria superaría los límites de Lambda incluso con 10240 MB, además de aumentar el riesgo de timeout.

**Parámetros actuales:**
- `transform`: chunks de 128 MB, flush cada 1,000,000 records
- `extract` / `clean`: chunks de 300,000 filas

---

## Por qué el router re-dispara a sí mismo con ZIPs

**Decisión:** Cuando el router detecta un ZIP, invoca `lmbd-unzip` de forma async (sin esperar), y cada archivo extraído se sube de vuelta al landing bucket, lo que genera nuevos S3 events que vuelven a disparar el router.

**Razón:** Paralelismo gratis. Si un ZIP contiene 5 archivos, los 5 se procesan en paralelo sin necesidad de orquestación adicional. El router no necesita saber que viene de un ZIP.

---

## Por qué Mastercard tiene un paso "Interpreter" que Visa no tiene

**Decisión:** El flujo Mastercard tiene `lmbd-mc-interpreter` como primer paso, antes del transform.

**Razón:** Los archivos IPM de Mastercard son binarios con estructura ISO-8583 (MTI + bitmaps + Data Elements), muy diferente al texto plano de ancho fijo de Visa. El interpreter traduce este formato a Parquets estructurados por MTI, que el transform puede procesar con la misma lógica que Visa.

**Complejidades adicionales del interpreter:**
- Archivos pueden venir "bloqueados" en bloques de 1014 bytes (requiere `unblock_1014`)
- Encoding configurable por cliente: `latin-1` o `cp500` (EBCDIC), definido en DynamoDB tabla `client`
- Mensajes delimitados por RDW (4 bytes big-endian)

---

## Por qué mc-interchange NO contrasta contra MTI 1644 (a diferencia de Visa vs VSS)

**Decisión:** `glue-mc-interchange` solo procesa MTIs 1240 y 1442 (transaccionales). No realiza Data Quality contra MTI 1644 (mensajes de liquidación MC).

**Razón:** El scope actual del interchange MC es la asignación de tarifas IAR a las transacciones. El contraste DQ contra los registros de liquidación 1644 es una funcionalidad adicional que puede incorporarse en una iteración posterior, cuando el pipeline transaccional esté completamente validado.

**Diferencia con Visa:** `glue-vi-interchange` sí contrasta contra registros VSS (TC 46) como parte del mismo job. En MC, el MTI 1644 existe en la capa CLN pero no se usa en el interchange actual.

**Inputs del interchange MC:** CLN (`400_IPM_{mti}_CLN`) + CAL (`500_IPM_{mti}_CAL`) + datos de referencia S3 (`currency/`, `exchange_rate/`, `mc_rules/`). Output: `600_IPM_{mti}_ITX`.

---

## Por qué mc-store fusiona Parquets por columnas nuevas (axis=1) y no por join de claves

**Decisión:** `lmbd-mc-store` fusiona CLN + CAL + ITX usando `pd.concat(frames, axis=1)` — merge horizontal por índice posicional. Solo añade columnas que no existen en el frame anterior.

**Razón:** El pipeline garantiza que CLN, CAL e ITX para el mismo MTI y archivo tienen exactamente el mismo número de filas en el mismo orden (no hay filtros ni reordenamiento entre etapas). Un join por clave añadiría complejidad y latencia sin beneficio. Si la garantía de orden se rompe en alguna etapa futura, esto se manifestará como datos incorrectos — señal de un bug upstream que hay que corregir allí.

**Alternativa descartada:** Join por columna de clave de transacción. Descartado por complejidad (requiere definir PK compuesta por MTI) y porque la garantía de orden ya existe por diseño del pipeline.

---

## Por qué el router extrae la fecha MC desde el trailer 695 en chunks (sin descarga completa)

**Decisión:** `extraer_fecha_mc()` en el router lee el archivo IPM en chunks de 8 MB buscando el primer trailer MTI 1644 / FC 695, extrae el PDS tag "0105" (file_idn) y deriva la fecha YYMMDD → YYYY-MM-DD. No descarga el archivo completo.

**Razón:** Consistencia con el patrón ya usado para archivos Visa (solo los primeros 50 bytes del header). Los archivos MC pueden superar 1.5 GB — descargarlos completos en el router para extraer una fecha sería prohibitivo en costo y tiempo. El trailer 695 con la fecha suele estar en los primeros pocos MB del archivo.

**Detalles de implementación:**
- Archivos bloqueados (`file_block=True`): chunks alineados a múltiplos de 1014 bytes + `_mc_unblock_chunk` antes de parsear
- Overlap de 8 KB entre chunks para no cortar mensajes en el límite de chunk
- Guardia `MAX_CHUNKS=100` (~800 MB máximo antes de retornar `datetime.utcnow()` como fallback
- Path de extracción: `DE48 del mensaje 695 → PDS tag "0105" → file_idn[3:9] → YYMMDD`

---

## Por qué los Glue jobs tienen args.json en el repositorio

**Decisión:** Cada Glue job tiene un `args.json` junto a su script con los `DefaultArguments` usados en AWS.

**Razón:** Permite reproducir exactamente la configuración del job (buckets, Spark conf, logging) desde el repositorio sin tener que consultar la consola AWS. También sirve como documentación de los argumentos que el Step Function debe pasar al invocar el job.

**Contenido típico:** rutas S3 de staging y reference, configuración de Spark (rolling logs, event logs), habilitación de métricas y job insights CloudWatch.

---

## Por qué el reporte usa Glue PySpark y no Athena ni el SP original en PostgreSQL

**Decisión:** El reporte de transacciones (`glue-vi-mc-reporting`) se implementa como Glue job PySpark, no como consulta Athena ni manteniéndolo en PostgreSQL.

**Razón:**
- El SP original (`analytics.generate_transaction_tables`) tardaba hasta 30 minutos para el cliente más grande, iterando fecha por fecha en un loop PL/pgSQL. PySpark lee el rango completo con partition pruning en una sola operación vectorizada.
- La lógica requiere joins contra múltiples tablas de referencia (country, exchange rates, BIN products) y lógica condicional por cliente (`duplicate_on_us`, `scheme_fee`) que supera lo que Athena puede expresar cómodamente en SQL estático.
- Elimina la dependencia de una base de datos PostgreSQL como capa intermedia — todo el reporte vive en S3.

**Athena descartada para este caso:** útil para consultas ad-hoc sobre los datos ya generados, no para generarlos. El reporte implica joins y transformaciones que en Athena requerirían CTEs complejos sin control de rendimiento.

---

## Por qué el reporting job lee directo desde S3 y no vía Glue catalog

**Decisión:** `glue-vi-mc-reporting` lee los Parquets de `s3-operational` usando `spark.read.parquet(path)` con filtros de partición, sin pasar por tablas del Glue catalog.

**Razón:** La estructura de paths `{client_id}/{brand}/{data_type}/file_type=*/date=*/` tiene el `client_id` como primer nivel del path, no como partición Hive. El crawler crearía una tabla separada por combinación `{client_id}/{brand}/{data_type}` — inmanejable para reportería multi-cliente. Leer directo desde S3 con path parametrizado por `client_id` es más simple y eficiente.

**Los crawlers siguen teniendo valor** para Athena (consultas ad-hoc sobre un cliente específico). Para el job de reportería, el path parametrizado es suficiente.

---

## Por qué se agrega content_hash como primera columna en cada Parquet del pipeline Visa

**Decisión:** Desde transform hasta interchange, cada Parquet del pipeline Visa incluye `content_hash` como primera columna.

**Razón:** Sin esta columna no hay forma de saber qué archivo originó cada fila al consultar las tablas en Athena. El `content_hash` es el MD5 del archivo fuente y ya se pasa como parámetro en todos los steps del Step Function — agregarlo al Parquet no tiene costo adicional.

**Implementación:**
- `lmbd-vi-transform`: `ParquetBatchWriter` recibe `content_hash` como parámetro; es el primer campo en el schema PyArrow
- `lmbd-vi-extract`: `extracted_chunk.insert(0, 'content_hash', content_hash)` tras cada batch
- `lmbd-vi-clean`: sin cambio — `_clean_chunk` itera todas las columnas del input y pasa `content_hash` automáticamente
- `glue-vi-calculate`: `"content_hash"` al inicio de `output_columns` en las 3 funciones (BASEII, SMS, VSS)
- `glue-vi-interchange`: `"content_hash"` al inicio de `interchange_cols`
- `lmbd-vi-store`: sin cambio — el merge hereda todas las columnas de CLN

**Alternativa descartada:** Usar `input_file_name()` de Spark en Athena/Glue para derivar el hash del path. Descartado porque los Lambdas (transform, extract, clean) usan pandas/PyArrow, no Spark, y los Glue jobs requerirían expresiones adicionales en cada consulta.

---

## Por qué el reporting job ejecuta un cliente por vez (no lista de clientes)

**Decisión:** `glue-vi-mc-reporting` (archivo: `glue/scripts/reports/get_transaction/get_transaction.py`) procesa un único cliente por ejecución. El parámetro es `--client_code` (singular).

**Razón:** Simplifica el job y lo hace más predecible en tiempo y memoria. Step Functions puede invocar múltiples ejecuciones en paralelo si se necesitan varios clientes. Los parámetros del job están dentro de `main()` con `global` declarations para las variables usadas por las funciones auxiliares (`OPERATIONAL_BUCKET`, `BUCKET_REF`, `DDB_CLIENT_TABLE`, `TABLE_SUFFIX`).

**Alternativa descartada:** `--client_codes` separados por coma con loop interno. Descartado porque mezcla tiempos de ejecución de clientes distintos en un solo job, complica el retry en caso de fallo parcial, y no aprovecha el paralelismo nativo de Step Functions.

---

## Por qué el reporting job no necesita el join M4 (m_interchange_rules_visa)

**Decisión:** `glue-vi-mc-reporting` no hace join contra una tabla de reglas de interchange para obtener `interchange_rule` (equivalente a `fee_descriptor` en el SP). Lee la columna directamente del Parquet operational.

**Razón:** El SP original hacía `JOIN m_interchange_rules_visa M4 ON M4.intelica_id = T3.intelica_id` para obtener `M4.fee_descriptor`. En el nuevo pipeline, `glue-vi-interchange` ya calcula y escribe `interchange_fee_descriptor` en la capa ITX, que `lmbd-vi-store` consolida en el Parquet final de operational. La columna ya está materializada — el join es redundante.

**Impacto:** Un join menos sobre una tabla de referencia potencialmente grande. El campo `interchange_fee_descriptor` en el Parquet operational es la fuente de verdad.

---

## Por qué lmbd-vi-store lee el CAL con _read_parquet_arrow en vez de _read_parquet_from_s3

**Decisión:** En `store_output` (`lambdas/visa/store/src/handler.py`), el Parquet CAL se lee con `_read_parquet_arrow()` (devuelve `pa.Table`) y se extrae el schema de columnas enteras antes de convertir a pandas.

**Razón:** `pq.read_table(...).to_pandas()` convierte INT64+nulls → float64 (numpy no tiene int nullable). Si el CAL tiene columnas enteras con nulls (como `timeliness`), al reconstruir la Arrow Table con `pa.Table.from_pandas(merged)`, PyArrow infiere `double`. El crawler de Glue luego detecta esas columnas como `double` en la capa operational en lugar de `bigint`.

**Solución:** Leer CAL como Arrow Table, capturar `_cal_int_cols = {nombre: tipo Arrow}`, convertir a pandas normalmente, y después de cada `pa.Table.from_pandas(merged)` restaurar los tipos enteros con `merged_table.set_column(..., col.cast(atype))`. Arrow castea `float64 null → int64 null` sin pérdida de datos.

**Alternativa descartada:** `table.to_pandas(use_nullable_dtypes=True)` — más limpio, pero requiere PyArrow ≥ 2.0 y el layer usa una versión anterior.
