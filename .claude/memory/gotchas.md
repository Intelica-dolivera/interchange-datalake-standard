# Gotchas y problemas conocidos

Problemas encontrados durante el desarrollo, con su causa raíz y solución recomendada. Verificar si siguen vigentes antes de actuar.

---

## mc-transform: timeout con múltiples MTIs (riesgo alto)

**Archivo:** `lambdas/mastercard/transform/src/handler.py`
**Detectado:** 2026-05-22

**Problema:** El handler procesa los 4 MTIs (1240, 1442, 1644, 1740) secuencialmente en una sola invocación. Si todos están presentes en el archivo, puede superar fácilmente el timeout de 400s.

**Solución recomendada:** Que Step Functions invoque el Lambda una vez por MTI, pasando el MTI como parámetro — igual que el patrón ya usado en el flujo Visa.

**Estado:** Pendiente de resolver antes de validación end-to-end.

---

## mc-transform: sin chunking en MTIs 1442, 1740 y 1644 (riesgo medio)

**Archivo:** `lambdas/mastercard/transform/src/handler.py`
**Detectado:** 2026-05-22

**Problema:** Solo `transform_ipm_1240` implementa chunking dinámico. Los MTIs 1442, 1740 y 1644 cargan el Parquet completo en memoria, lo que puede causar OOM en archivos grandes.

**Solución recomendada:** Replicar el patrón de chunking de `transform_ipm_1240` en los otros tres MTIs.

**Estado:** Pendiente.

---

## mc-transform: EphemeralStorage /tmp insuficiente (riesgo medio)

**Archivo:** `lambdas/mastercard/transform/src/handler.py`  
**Config:** `lambdas/mastercard/transform/config.json`
**Detectado:** 2026-05-22

**Problema:** `transform_ipm_1240` escribe un Parquet completo en `/tmp` antes de subirlo a S3. El EphemeralStorage por defecto es 512 MB, insuficiente para archivos Mastercard grandes.

**Solución recomendada:** Aumentar EphemeralStorage a 2048 MB+ en la config del Lambda, o cambiar la escritura para hacer stream directo a S3 (sin pasar por `/tmp`).

**Estado:** Pendiente.

---

## mc-transform: variable de entorno DDB_MASTERCARD_FIELDS_TABLE no declarada en config.json (bug latente)

**Archivo:** `lambdas/mastercard/transform/config.json`
**Detectado:** 2026-05-22

**Problema:** El código usa `DDB_MASTERCARD_FIELDS_TABLE` para consultar la tabla de campos Mastercard en DynamoDB, pero esta variable no está declarada en `config.json` ni en `env-vars.json`. Cae al valor hardcodeado `"itl-0004-itx-dev-dynamo-mastercard_fields-02"`, lo que romperá en ambientes distintos a dev.

**Solución recomendada:** Agregar `DDB_MASTERCARD_FIELDS_TABLE` a `config.json` y `env-vars.json` igual que las otras variables de entorno del Lambda.

**Estado:** Pendiente — bug latente que se manifestará al desplegar en ambiente empresarial.

---

## itx-extract comparte el rol IAM del router (deuda técnica)

**Detectado:** 2026-04-08 (CHANGELOG v1.0.0)

**Problema:** `lmbd-vi-extract` no tiene un rol IAM propio — comparte `itx-lambda-router-role`. Esto viola el principio de mínimo privilegio.

**Solución recomendada:** Crear `itx-lambda-extract-role` con solo los permisos que extract necesita (S3 read/write staging, DynamoDB read visa-fields).

**Estado:** Pendiente (documentado en CHANGELOG como tarea para el nuevo ambiente).

---

## glue-vi-calculate: Py4JError causado por toPandas() en load_visa_ardef — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py`
**Detectado:** 2026-06-02

**Problema:** `load_visa_ardef` descargaba el ARDEF filtrado al driver con `.toPandas()` y luego hacía deduplicación y eliminación de rangos solapados en pandas. Con archivos grandes, presionaba la heap del driver causando OOM → JVM caía → la siguiente llamada a `logger.info()` vía Py4J lanzaba `Py4JError: An error occurred while calling o<N>.info`.

**Solución aplicada (2026-06-02):** Migración completa a Spark — eliminado `toPandas()`, `import pandas as pd` y el parámetro `ardef_pd` de todas las firmas. Las operaciones de deduplicación y eliminación de solapamientos ahora usan `Window.partitionBy` + `row_number()` y `F.lag()`. El ARDEF nunca sale de los executors.

**Estado:** Resuelto. Si vuelve a aparecer `Py4JError` en este job, buscar en CloudWatch `Java heap space` o `ExecutorLostFailure` justo antes.

---

## glue-mc-interchange: filtra por file_id para no reprocesar ejecuciones anteriores

**Archivo:** `glue/scripts/mastercard/interchange/interchange.py`
**Detectado:** 2026-06-02 (implementación inicial)

**Problema (resuelto en la implementación):** Sin filtro por `file_id`, el job listaba TODOS los Parquets de la partición `file_type=X/date=YYYY-MM-DD` y reprocesaba archivos de ejecuciones anteriores del mismo día, actualizando su Last-Modified innecesariamente y potencialmente mezclando resultados de diferentes archivos fuente.

**Solución aplicada:** Filtrar los archivos listados por `stem_from_uri(path).upper().startswith(file_id.upper())` antes de procesarlos. Se aplica tanto a los archivos TXN (CLN) como a los CAL.

**Estado:** Resuelto. Comportamiento correcto en producción — cada ejecución del Step Function procesa únicamente sus propios archivos.

**Nota:** Este mismo patrón debe verificarse en `glue-vi-interchange` si alguna vez se presenta el mismo síntoma.

---

## glue-vi-calculate: timeliness debe ser LongType (bigint), NO IntegerType — HIVE_PARTITION_SCHEMA_MISMATCH

**Archivo:** `glue/scripts/visa/calculate/calculate.py`
**Detectado:** 2026-06-05

**Problema:** Si `calc_timeliness_draft` o `calc_timeliness_sms` usan `.cast(IntegerType())`, los Parquets nuevos escriben `int` (INT32). Los archivos existentes en S3 tienen `bigint` (INT64 / LongType — resultado natural de las aritméticas con `F.floor()` + `F.datediff()`). Al re-correr el crawler, la tabla queda con tipo `int` pero las particiones viejas siguen siendo `bigint`. Athena lanza:
```
HIVE_PARTITION_SCHEMA_MISMATCH: column 'timeliness' declared as type 'int',
but partition declared column 'timeliness' as type 'bigint'
```

**Solución aplicada (2026-06-05):** Usar `.cast(LongType())` para `timeliness` — tanto en `calc_timeliness_draft` como en `calc_timeliness_sms`. Todos los archivos (viejos y nuevos) quedan como `bigint`.

**Si vuelve a aparecer:** Verificar que el script en S3 use `LongType()`. Si hay particiones mixtas, editar la tabla en Glue catalog y forzar `bigint` manualmente antes de re-correr el crawler.

**Estado:** Resuelto.

---

## glue-vi-calculate: tipos explícitos en funciones de cálculo numérico

**Archivo:** `glue/scripts/visa/calculate/calculate.py`
**Detectado:** 2026-06-05

**Problema:** Sin `.cast()` explícito en columnas numéricas, Spark infiere tipos que el crawler de Glue detecta incorrectamente en Athena (e.g., `double` en lugar de `int`).

**Solución aplicada (2026-06-05):**

| Función | Cast aplicado |
|---------|--------------|
| `calc_business_transaction_type_draft` | `.cast(IntegerType())` |
| `calc_reversal_indicator_draft` | `.cast(IntegerType())` |
| `calc_reversal_indicator_sms` | `.cast(IntegerType())` |
| `calc_surcharge_amount` | `.cast(DoubleType())` + `F.lit(0.0)` |
| `calc_timeliness_draft` | `.cast(LongType())` — ver gotcha anterior |
| `calc_timeliness_sms` | `.cast(LongType())` — ver gotcha anterior |

**Regla:** Toda nueva función de cálculo numérico debe terminar con `.cast(TipoExplícito)`.

**Estado:** Resuelto.

---

## lmbd-vi-store: columnas enteras del CAL se escriben como double en operational — RESUELTO

**Archivo:** `lambdas/visa/store/src/handler.py`
**Detectado:** 2026-06-05

**Problema:** El crawler de Glue detectaba `timeliness` (y cualquier otra columna `LongType`/`IntegerType` del CAL que tuviera nulls) como `double` en la capa operational, en vez de `bigint`/`int`.

**Causa raíz:** `pq.read_table(...).to_pandas()` convierte automáticamente columnas INT64+nulls a `float64` (numpy no tiene tipo entero nullable). Al reconstruir la tabla con `pa.Table.from_pandas(merged)`, PyArrow infiere `double` desde `float64`. El `ParquetWriter` fija ese schema desde el primer batch y todos los archivos quedan como `double`.

**Solución aplicada (2026-06-05):** En `store_output`, el CAL se lee con `_read_parquet_arrow()` (devuelve `pa.Table`) en lugar de `_read_parquet_from_s3()`. Antes de convertir a pandas, se extrae `_cal_int_cols = {nombre: tipo}` para todas las columnas enteras del schema Arrow. En cada batch del loop, después de `pa.Table.from_pandas(merged)`, se restauran los tipos enteros con `merged_table.set_column(..., col.cast(atype))`. Arrow soporta `float64 null → int64 null` sin pérdida de datos.

**Por qué no se usó `use_nullable_dtypes=True`:** El layer usa una versión de PyArrow < 2.0 que no soporta ese parámetro.

**Si vuelve a aparecer:** Verificar que `_cal_int_cols` se construya correctamente antes del loop. Si hay nuevas columnas enteras en el CAL que queden como double, revisar que el schema del CAL Arrow tenga `is_integer(f.type) == True` para esas columnas.

**Estado:** Resuelto.

---

## glue-mc-interchange: solo procesa MTIs 1240 y 1442 (1644 y 1740 excluidos)

**Archivo:** `glue/scripts/mastercard/interchange/interchange.py`
**Detectado:** 2026-06-02

**Comportamiento:** El job llama a `run_interchange_mti()` únicamente para MTIs 1240 y 1442. Los MTIs 1644 (liquidación) y 1740 (fee collection) no tienen capa ITX generada por este job.

**Impacto en mc-store:** `MTIS_WITH_ITX = frozenset({"1240", "1442"})` — el store no intentará buscar `600_IPM_1644_ITX` ni `600_IPM_1740_ITX`, lo que es correcto.

**Estado:** Por diseño. No es un bug. Ver decisión en `decisions.md` sobre por qué no se contrasta contra 1644.
