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
