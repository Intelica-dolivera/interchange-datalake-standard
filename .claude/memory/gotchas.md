# Gotchas y problemas conocidos

Problemas encontrados durante el desarrollo, con su causa raíz y solución recomendada. Verificar si siguen vigentes antes de actuar.

---

## glue-vi-interchange: fillna(0.0) en fee_min/fee_cap zeroeaba fees positivos — RESUELTO

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `process_pandas_partitions`)
**Detectado:** 2026-06-09

**Síntoma:** `interchange_fee_amount` era 0 para transacciones que matcheaban reglas sin cap explícito (`fee_cap=NaN`). En la comparación por jurisdiction/source_currency, jurisdicciones off-us EUR mostraban fee_amount ≈289 USD menos que el legacy.

**Causa raíz:**

```python
# ANTES (bug):
result_pdf["interchange_fee_cap"] = result_pdf["interchange_fee_cap"].fillna(0.0).astype(float)
result_pdf["interchange_fee_min"] = result_pdf["interchange_fee_min"].fillna(0.0).astype(float)
```

Reglas sin cap definen `fee_cap=NaN` en pandas. `fillna(0.0)` lo convierte a `0.0`. Spark recibe `0.0` (no NULL), por lo que `coalesce` no actúa:
```python
F.coalesce(F.col("interchange_fee_cap"), F.lit(float("inf")))  # → coalesce(0.0, +inf) = 0.0
F.least(F.col("interchange_fee_amount"), 0.0)                  # → min(15.50, 0.0) = 0.0
```
Todos los fees positivos de esas reglas quedaban en cero. El mismo problema con `fee_min=NaN → 0.0` flooreaba fees negativos innecesariamente.

**Solución aplicada (2026-06-09):**
```python
# DESPUÉS (correcto):
result_pdf["interchange_fee_cap"] = result_pdf["interchange_fee_cap"].astype(float)
result_pdf["interchange_fee_min"] = result_pdf["interchange_fee_min"].astype(float)
```
NaN de pandas → NULL en Spark → `coalesce(NULL, ±inf)` → sin restricción. Para reglas con cap explícito (ej. `fee_cap=0.04`), el valor se conserva y actúa como cap real.

**Si vuelve a aparecer (fees en 0 para reglas que deberían tener fee positivo):** Verificar que no haya `fillna(0.0)` sobre `interchange_fee_cap` o `interchange_fee_min` antes del yield en `process_pandas_partitions`. El patrón correcto: solo `astype(float)`, sin fillna.

**Estado:** Resuelto en código local y subido a S3 (2026-06-09). Pendiente validar re-run.

---

## glue-vi-interchange: matching incorrecto intelica_id ATM JPY — regla 1055 en vez de 1065 — PENDIENTE

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (motor de reglas `_apply_default` / `_evaluate_rules_pandas`)
**Detectado:** 2026-06-09

**Síntoma:** En la comparación de `sum(interchange_fee_amount)` por jurisdiction/source_currency, la diferencia residual de −29.64 para interregional JPY (source_currency=392) se debe a que el nuevo sistema asigna `intelica_id=1055` ("ATM AF") mientras el legacy asigna `intelica_id=1065` ("ATM AF JPN").

**Detalle de las reglas vigentes al 2026-01-03:**

| intelica_id | fee_descriptor | fee_variable | fee_fixed | fee_currency |
|---|---|---|---|---|
| 1055 | ATM AF | 0.0015 | — | None (source_ccy) |
| 1065 | ATM AF JPN | 0.0015 | 0.50 | USD |

Simulación para source_amount=20,220 JPY:
- Legacy (1065): `0.0015 × 20,220 × exchange(JPY→USD) + 0.50 = 0.19 + 0.50 = 0.69 USD`
- Nuevo (1055): `0.0015 × 20,220 = 30.33 JPY`

Nota: los fee_amounts están en **monedas distintas** (USD vs JPY) — no son comparables como número directo.

**Causa probable:** La regla 1065 "ATM AF JPN" tiene alguna condición que la restringe a transacciones de Japón (issuer_country, acquirer_country, o merchant_country). Esa condición existe en `visa_rules` pero el motor de reglas del nuevo sistema no la está evaluando correctamente o no está presente en el `calculate.parquet` para esa transacción.

**Para investigar:** Comparar los campos de condición entre la regla 1065 y la 1055 en `visa_rules.parquet` (ambas vigentes al 2026-01-03) para identificar qué campo diferencia "ATM AF JPN" de "ATM AF". Verificar que ese campo tenga el valor correcto en `calculate.parquet` para la transacción en cuestión.

**Estado:** Pendiente de investigación. La diferencia de −29.64 en la comparación global es 1 transacción (count=1).

---

## glue-vi-interchange: dirección del exchange_value — pendiente validar convención

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `calculate_fee_amounts`)
**Detectado:** 2026-06-09

**Contexto:** Existen dos fórmulas posibles para `interchange_fee_amount`, con resultados distintos en transacciones cross-currency:

| Sistema | Fórmula | Moneda del resultado |
|---|---|---|
| Legacy PostgreSQL | `fee_variable × (source_amount × exchange_value) + fee_fixed` | fee_currency |
| Prototipo local | `fee_variable × source_amount + fee_fixed × exchange_value` | source_currency (si exchange_value = source_ccy/fee_ccy) |
| Glue actual | `fee_variable × source_amount + fee_fixed × exchange_value` | depende de convención |

El usuario prefiere que el fee se exprese en **source_currency** ("la regla se adapta a la moneda de la transacción"). La fórmula del prototipo es consistente con eso SI `exchange_value` en la tabla S3 almacena `source_ccy/fee_ccy` (convención inversa a la del legacy).

**Para validar:** Leer `s3://itl-0004-itx-dev-intchg-02-s3-reference/exchange_rate/data.parquet`, filtrar `currency_from=EUR, currency_to=USD`, ver si `exchange_value ≈ 1.08` (fee_ccy/source_ccy, convención legacy) o `≈ 0.926` (source_ccy/fee_ccy, convención prototipo).

**Estado:** Pendiente — validar convención del exchange_value antes de decidir si la fórmula actual de `calculate_fee_amounts` es correcta.

---

## glue-vi-calculate: calc_timeliness_draft fórmula de domingos tenía off-by-one — no cuadraba con legacy — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py` (función `calc_timeliness_draft`)
**Detectado:** 2026-06-09

**Síntoma:** Los valores de `timeliness` no coincidían con el legacy PostgreSQL para transacciones donde el residuo de la ventana (`remaining_days`) era exactamente igual a `days_to_next_sunday`. En esos casos el legacy daba N, el Glue job daba N−1 (1 domingo de más contado).

**Causa raíz:** La fórmula original construía la ventana `[purchase+1, central−1]` y contaba domingos con `full_weeks + extra_sunday`:

```python
_days_between = datediff(end_for_sundays, start_for_sundays) + 1
_full_weeks   = floor(days_between / 7)
_remaining    = days_between % 7
_dts          = when(start_dow == 1, 0).otherwise(8 - start_dow)  # días al próximo domingo
_extra_sunday = when(days_between > 0 AND remaining >= dts, 1).otherwise(0)
```

El bug: cuando `remaining == dts`, el próximo domingo cae en la posición `full_weeks*7 + dts = days_between` — exactamente fuera del intervalo (índices válidos: `0..days_between−1`). La condición `>=` lo contaba igual.

**Ejemplo concreto:**
- purchase=2026-01-04 (Dom), central=2026-01-18 (Dom), total_days=14
- Ventana [05-ene (Lun), 17-ene (Sáb)] — un único domingo: 11-ene
- Old: full_weeks=1, remaining=6, dts=6 → `6 >= 6` → extra=1 → sundays=2 ✗
- New: `max(0, floor((14 − 1 + 6 − 6) / 7)) = floor(13/7) = 1` ✓

**Solución aplicada (2026-06-09):** Reescritura a fórmula directa con offset (comentario en el diff):

```python
_start_dow     = dayofweek(purchase_date + 1)       # Spark: Dom=1..Sáb=7
offset         = (8 - start_dow) % 7                # días desde start hasta el primer domingo
_sundays_count = max(0, floor((total_days − 1 + 6 − offset) / 7))
```

Derivación: en una ventana de `ws = total_days − 1` días con primer domingo en posición `offset` (0-indexed), el número de domingos es `max(0, floor((ws − offset) / 7) + 1) = max(0, floor((ws + 6 − offset) / 7))`.

**Si vuelve a aparecer (timeliness 1 menos que legacy para algunos registros, patrón relacionado con day-of-week de purchase+1):** La señal es que la discrepancia aparece solo cuando `(total_days − 1) % 7 == (8 − start_dow) % 7`. Cualquier lógica `remaining >= days_to_next_sunday` tiene este off-by-one — usar `>` o bien la fórmula directa con offset.

**Estado:** Resuelto en código local (2026-06-09). Pendiente subir a S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py`) y re-ejecutar `glue-vi-calculate`.

---

## glue-vi-interchange: _apply_default() convertía NaN a cadena "nan" en columnas no-SPACE — filas excluidas de reglas válidas — RESUELTO

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `_apply_default`)
**Detectado:** 2026-06-09

**Síntoma:** Transacciones con valores NULL en columnas de condición que no pertenecen a `COLUMN_GROUP_SPACE` no matcheaban reglas válidas, cayendo en la regla fallback/default aunque todas las demás condiciones se cumplieran.

**Causa raíz:** La normalización de columnas no-SPACE era:

```python
temp = batch[condition_name].astype(str).str.strip()
temp = temp.mask(temp.str.len() == 0, "BLANK")
```

Pandas convierte `NaN → "nan"` con `.astype(str)`. Como `len("nan") == 3 ≠ 0`, el `.mask(len == 0, "BLANK")` no sustituye el valor — la columna queda con la cadena `"nan"`. Al contrastarla contra `valid_values` (e.g. `['Y', 'N', 'BLANK']`) no hay match → fila excluida. La intención del token `"BLANK"` es exactamente representar "campo vacío o ausente" — un NULL debe mapearse a `"BLANK"`, no a `"nan"`.

**Solución aplicada (2026-06-09):**

```python
temp = batch[condition_name].fillna("").astype(str).str.strip()
temp = temp.mask(temp.str.len() == 0, "BLANK")
```

`fillna("")` antes de `astype(str)` garantiza: `NaN → "" → "" (strip) → len=0 → "BLANK"`.

**Si vuelve a aparecer (filas con campos NULL no matchean reglas donde deberían):** Verificar que no haya `astype(str)` directo sobre columnas con nulls antes de normalizar. El patrón seguro es siempre `fillna("").astype(str).str.strip()` — tanto en `_apply_default` como en cualquier función nueva de normalización de condiciones.

**Estado:** Resuelto en código local (2026-06-09). Pendiente subir a S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py`) y re-ejecutar `glue-vi-interchange`.

---

## glue-vi-interchange: content_hash se perdía en el Parquet ITX por mapInPandas — RESUELTO (pendiente validar tras re-run)

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `evaluate_interchange_fees`)
**Detectado:** 2026-06-08

**Síntoma:** El usuario reportó que `content_hash` no aparecía en el Parquet de interchange (`itx.parquet`), aunque la columna sí figura en `interchange_cols` (lista de columnas finales de `process_output`).

**Causa raíz:** `evaluate_interchange_fees()` usa `mapInPandas()`, que **reemplaza por completo el schema** del DataFrame — cualquier columna no declarada explícitamente en `OUTPUT_COLS` y `output_schema` se descarta silenciosamente, sin error. `content_hash` SÍ llega como columna de entrada (viene de `cln_df`/`merged`, propagado desde transform→clean→calculate — ver `decisions.md` → "Por qué se agrega content_hash..."), pero `OUTPUT_COLS`/`output_schema` no lo declaraban, así que `yield result_pdf[OUTPUT_COLS]` lo eliminaba antes de que existiera en `result`. Luego `existing_cols = [c for c in interchange_cols if c in result.columns]` lo filtraba sin avisar — el job terminaba en SUCCESS, conteo correcto, pero sin la columna.

**Solución aplicada (2026-06-08):**
1. Agregado `"content_hash"` como primer elemento de `OUTPUT_COLS` (línea ~535)
2. Agregado `StructField("content_hash", StringType(), True)` como primer campo de `output_schema` (línea ~584)

**Si vuelve a aparecer (columna ausente en el Parquet final pese a estar en la lista de columnas finales):** sospechar de un `mapInPandas`/`applyInPandas` intermedio que reemplaza el schema — verificar que la columna esté declarada tanto en la lista de salida del iterador como en el `StructType` del schema, no solo en la selección final.

**Estado:** Resuelto en código y subido a S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py`, 2026-06-08). Pendiente re-ejecutar `glue-vi-interchange` y validar que `content_hash` aparece como primera columna del `itx.parquet` resultante.

---

## glue-vi-interchange: _apply_default() destruía el token "Space" (espacio literal) — transacciones GR caían en regla fallback — RESUELTO (pendiente validar tras re-run)

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `_apply_default`)
**Detectado:** 2026-06-08

**Síntoma:** Transacciones de Grecia (GR) con `acceptance_terminal_indicator` = espacio literal (`' '`) no matcheaban la regla `intelica_id=39` ("GR SECURE CR", criterio `acceptance_terminal_indicator='Space,9'`) — caían en la regla fallback/default `intelica_id=63` ("GR NON-SEC CR", `program_default='Y'`).

**Causa raíz:** En `_apply_default()`, dentro del loop que parsea `value_list` había un `value = value.strip()` extra que **no existe** en la versión validada del prototipo local (`tst_files/interchange_local.py` → `_apply_condition_default`). Para el criterio `"Space,9"`:
- Tras `replace("SPACE", " ")` + `split(",")`: `[' ', '9']`
- Con el `.strip()` extra: `' '` se convierte en `''` → `valid_values = ['', '9']`
- Como `acceptance_terminal_indicator` está en `COLUMN_GROUP_SPACE` (su valor de transacción se conserva como `' '` literal, sin normalizar/strip), el filtro `_normalized.isin(valid_values)` excluye toda transacción con `' '` porque `' ' not in ['', '9']`

**Cómo se detectó:** Comparación línea por línea de `_apply_default` (Glue) vs `_apply_condition_default` (local) — la única diferencia relevante era ese `.strip()` extra. Se confirmó vía regex sobre `tst_files/visa_rules.parquet` que **ningún criterio real contiene comas seguidas de espacio** — el `.strip()` no tenía caso de uso legítimo, era código incidental que introdujo la regresión.

**Validación contra producción:** En el operational `D44C4427AED04C1E078AA86B275060FA.parquet` (jurisdiction_assigned=GR, 206,718 filas), 21,085 transacciones con `acceptance_terminal_indicator=' '` cayeron en la regla fallback 63. De ellas, **524 cumplían absolutamente TODAS las demás condiciones de la regla 39** (transaction_code, transaction_code_qualifier, account_funding_source, product_id, authorization_code, timeliness, pos_environment_code, pos_terminal_capability, pos_entry_mode, cardholder_id_method, authorization_response_code, reimbursement_attribute) — prueba directa de mala clasificación (`fee_descriptor='GR NON-SEC CR'` en vez de `'GR SECURE CR'`) causada únicamente por este bug.

**Solución aplicada (2026-06-08):** Eliminado `value = value.strip()` (línea 300) — alineando `_apply_default` con el comportamiento ya validado de `_apply_condition_default` del prototipo local.

**Si vuelve a aparecer (criterios "Space"/espacio literal no matchean):** verificar que ningún `.strip()` o normalización adicional se aplique a los valores de `value_list` después del `replace("SPACE", " ")` — el espacio literal debe sobrevivir intacto hasta el `isin()`.

**Estado:** Resuelto en código y subido a S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py`, 2026-06-08). Pendiente re-ejecutar `glue-vi-interchange` y validar que las transacciones GR con `acceptance_terminal_indicator=' '` que cumplen el resto de condiciones de la regla 39 ahora obtienen `interchange_intelica_id=39` (antes: 63).

---

## glue-vi-calculate: load_visa_ardef() vaciaba el ARDEF por to_date() sin formato — campos ARDEF quedaban 100% null — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py` (función `load_visa_ardef`)
**Detectado:** 2026-06-06

**Síntoma:** `calculate.parquet` se generaba correctamente (mismo Nº de filas que `clean.parquet`) pero los 10 campos derivados del cruce con ARDEF (`ardef_country`, `product_id`, `funding_source`, `b2b_program_id`, `fast_funds`, `nnss_indicator`, `product_subtype`, `technology_indicator`, `travel_indicator`, `issuer_country`) salían **100% null**.

**Causa raíz:** `effective_date` en `visa_ardef/data.parquet` viene como string en formato `yyyyMMdd` (ej. `'20131018'`, las 1,710,400 filas), pero el código llamaba `F.to_date(F.col("effective_date"))` **sin especificar formato**. `to_date()` sin formato espera ISO `yyyy-MM-dd`, así que devuelve `NULL` para el 100% de las filas. El filtro posterior `effective_date <= file_date` descarta entonces TODO el ARDEF (queda vacío), y el join produce 100% nulls en los campos derivados.

Adicionalmente había un pre-filtro (antes de convertir a `DateType`) que comparaba `effective_date` (string `yyyyMMdd`) directamente contra `file_date_str` (string `yyyy-MM-dd`) — comparación lexicográfica de formatos distintos, también incorrecta (cualquier dígito `'0'-'9'` > `'-'` en ASCII, así que fechas del mismo año del archivo se excluían incorrectamente).

**Cómo se detectó:** Replicando `load_visa_ardef()` + el range join en pandas (`tst_files/debug_ardef_join.py`) y comparando **valor a valor** (alineado por `record`, no por posición — Spark reordena filas) contra `calculate.parquet`. El join local daba 100% match contra los 553,929 rangos ARDEF válidos; el `calculate.parquet` real tenía 100% null en esos campos — 0% de coincidencia, confirmando que el ARDEF llegaba vacío al job real.

**Solución aplicada (2026-06-06):**
1. `F.to_date(F.col("effective_date"))` → `F.to_date(F.col("effective_date"), "yyyyMMdd")` — mismo patrón ya usado correctamente en `glue/scripts/mastercard/calculate/calculate.py:826-829` (`F.to_date(_fdt_str, "yyMMdd")` / `"yyyyMMdd"` según longitud).
2. Se eliminó el pre-filtro de strings con formatos distintos — el filtro real ya existe después de convertir ambas fechas a `DateType` (paso 3 de la función), que es format-agnostic y correcto.

**Si vuelve a aparecer (campos ARDEF en null):** Verificar primero que `ardef.count()` después de `load_visa_ardef()` no sea 0 o anormalmente bajo (el log `"ARDEF loaded: {count} valid ranges..."` lo reporta). Si es 0, sospechar de un cambio de formato en `effective_date`/`valid_until` del `data.parquet` de referencia — confirmar el formato real con una muestra antes de tocar el `to_date()`.

**Estado:** Resuelto — pendiente re-ejecutar `glue-vi-calculate` para regenerar `calculate.parquet` y validar con `debug_ardef_join.py` que el match sube a ~100%.

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

---

## lmbd-vi-clean: _parse_dates() lógica incorrecta para campos de fecha YDDD y MMDD — RESUELTO

**Archivo:** `lambdas/visa/clean/src/handler.py` (función `_parse_dates`)
**Detectado:** 2026-06-08

**Síntoma 1 — `central_processing_date` / `account_reference_number_date` con valores ≈ -10 años:**
La lógica anterior `!YDDD` usaba "compute-then-correct": construir fecha tentativa (`decade + Y + DDD`) y si resultado > `file_date` → restar **10 años**. Para `campo='6004'` con `file_date=2026-01-03`: decodifica → `2026-01-04` > `2026-01-03` → resta 10 años → **2016-01-04**. Esto causaba `timeliness` del orden `-3653` días (≈ -10 años) en ~14% de los registros.

**Síntoma 2 — `purchase_date` retrocedía 1 año cuando debería conservar el año actual:**
La lógica anterior prepend año de `file_date`, y si resultado > `file_date` → restaba 1 año. Para `campo='0104'` (4-ene) con `file_date=2026-01-03`: `2026-01-04 > 2026-01-03` → restaba 1 año → **2025-01-04** (incorrecto). Causa: `purchase_date` usa formato MMDD y puede ser 1-2 días posterior al `file_date` dentro del mismo mes (el VIC procesa en días consecutivos). La spec Visa no prohíbe eso.

**Síntoma 3 — `conversion_date` aparecía con fecha futura (+1 año respecto al valor correcto):**
Misma lógica `!YDDD` sin restricción decodificaba `campo='6004'` como `2026-01-04`, cuando el correcto es **2025-01-04**. `conversion_date` es la fecha del archivo de tasas usado — una tasa del futuro es imposible según la spec Visa.

**Causa raíz:** La estrategia "compute-then-correct" (restar años si el resultado es futuro) es incorrecta para todos los casos. Los campos YDDD pueden legítimamente superar `file_date` en 1-2 días (VIC multi-día), y `purchase_date` MMDD no debe compararse por fecha completa sino solo por mes.

**Solución aplicada (2026-06-08):** Reescritura completa de `_parse_dates()` con tres estrategias derivadas de la spec Visa y del sistema legacy (adapters.py):

| Formato | Campos | Estrategia |
|---------|--------|-----------|
| `!YDDD` | `central_processing_date`, `account_reference_number_date` | `decade_of(file_date) + Y + DDD`, parsea `%y%j`. Sin corrección posterior — el resultado puede ser mayor a `file_date`. |
| `!YDDD_MAX` | `conversion_date` | Idéntico a `!YDDD` + cap: si resultado > `file_date` → restar 1 año. |
| `!MMDD` | `purchase_date` | Infiere año comparando **solo el mes**: `MM_campo <= MM_file_date` → mismo año; `MM_campo > MM_file_date` → año anterior. |

Todos los formatos: `'0000'` → `file_date` (proxy para evitar `NaT` en cálculos de `timeliness`).

**DynamoDB actualizado (2026-06-08):** `itl-0004-itx-dev-dynamo-visa_fields-02`, registro `type_record=draft / column_name=conversion_date` → `date_format` cambiado de `!YDDD` a `!YDDD_MAX`.

**Esquema real de claves de `visa_fields-02`:** HASH=`type_record`, RANGE=`column_name` (la documentación en CLAUDE.md decía `field_id` — error de documentación; la tabla tiene un GSI `type-record-index` que el código usa para queries).

**Validación completa vs PostgreSQL legacy (file_date=2026-01-03, 553,929 registros BASEII):**
```
purchase_date            !MMDD        2026-01-04    52502    52502  OK
purchase_date            !MMDD        2026-01-03    92346    92346  OK
conversion_date          !YDDD_MAX    2025-01-04    86519    86519  OK
conversion_date          !YDDD_MAX    2025-01-05   106026   106026  OK
central_processing_date  !YDDD        2026-01-04    86850    86850  OK
central_processing_date  !YDDD        2026-01-05   109105   109105  OK
account_ref_number_date  !YDDD        2026-01-04    77178    77178  OK
account_ref_number_date  !YDDD        2026-01-05    12607    12607  OK
```
0 nulls en todos los campos, 100% coincidencia con legacy.

**Si vuelven a aparecer fechas con año incorrecto en campos de fecha Visa:**
- `timeliness` ≈ ±3650 → alguna copia antigua del handler con la lógica "restar 10 años" — verificar que el deployment apunta al código correcto
- `purchase_date` un año atrás → revisar que la comparación sea solo por mes (`src_month > reference_date.month`), no por fecha completa
- `conversion_date` un año adelante → verificar `date_format=!YDDD_MAX` en DynamoDB y que el código aplique `future_mask`

**Estado:** Resuelto — `handler.py` subido al Lambda `lmbd-vi-clean` por el usuario (2026-06-08). Script de validación: `tst_files/debug_clean_dates.py`.
