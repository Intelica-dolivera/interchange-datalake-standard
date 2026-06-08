# Ejecución manual de pasos del pipeline (debugging)

Contexto: para depurar el pipeline es mucho más rápido ejecutar cada paso a mano que volver a subir el archivo al S3 landing y esperar que el router + Step Functions arranquen todo desde cero.

---

## Prerequisito — autenticación AWS

```powershell
aws sso login --profile itx-dev
$env:AWS_PROFILE = "itx-dev"   # opcional, evita pasar --profile en cada comando
```

---

## Flujo de trabajo para Glue Jobs

### 1. Preparar los argumentos

Pegar los argumentos de la ejecución (copiados desde el payload del Step Function o desde un run anterior) en:

```
tst_files/vi-calculate-run-test.txt   # o el .txt que corresponda al job
```

Formato del archivo: clave y valor en líneas alternas, sin separadores:
```
--content_hash
D44C4427AED04C1E078AA86B275060FA
--client_id
EBGR
...
```

### 2. Generar el JSON de argumentos

```powershell
python tst_files/generate_glue_args.py
# genera tst_files/vi-calculate-run-args.json

# con paths custom:
python tst_files/generate_glue_args.py mi_args.txt mi_args.json
```

### 3. Lanzar el job

```powershell
aws glue start-job-run `
  --profile itx-dev `
  --job-name itl-0004-itx-dev-intchg-02-glue-vi-calculate `
  --arguments "file://tst_files/vi-calculate-run-args.json"
# devuelve: { "JobRunId": "jr_..." }
```

### 4. Verificar estado del job

```powershell
aws glue get-job-run `
  --profile itx-dev `
  --job-name itl-0004-itx-dev-intchg-02-glue-vi-calculate `
  --run-id jr_XXXX `
  --query "JobRun.{State:JobRunState,Error:ErrorMessage,Start:StartedOn}" `
  --output table
```

---

## Nombres reales de los Glue Jobs

| Job | Nombre AWS |
|-----|-----------|
| vi-calculate | `itl-0004-itx-dev-intchg-02-glue-vi-calculate` |
| vi-interchange | `itl-0004-itx-dev-intchg-02-glue-vi-interchange` |
| mc-calculate | `itl-0004-itx-dev-intchg-02-glue-mc-calculate` |
| mc-interchange | `itl-0004-itx-dev-intchg-02-glue-mc-interchange` |

---

## Crawlers

### Lanzar crawler

```powershell
aws glue start-crawler `
  --profile itx-dev `
  --name itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa
```

Sin output = arrancó correctamente.

### Verificar estado del crawler

```powershell
aws glue get-crawler `
  --profile itx-dev `
  --name itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa `
  --query "Crawler.{State:State,LastStatus:LastCrawl.Status,Start:LastCrawl.StartTime}" `
  --output table
```

Estados posibles: `READY` (idle), `RUNNING`, `STOPPING`.

### Nombres reales de los crawlers

| Crawler | Nombre AWS |
|---------|-----------|
| Staging EBGR VISA | `itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa` |

---

## Lambdas (ejecución directa)

```powershell
# Invocación sync (espera resultado):
aws lambda invoke `
  --profile itx-dev `
  --function-name itl-0004-itx-dev-intchg-02-lmbd-vi-calculate `
  --payload "file://tst_files/payload.json" `
  --cli-binary-format raw-in-base64-out `
  response.json
cat response.json

# Invocación async (fire-and-forget):
aws lambda invoke `
  --profile itx-dev `
  --invocation-type Event `
  --function-name itl-0004-itx-dev-intchg-02-lmbd-vi-transform `
  --payload "file://tst_files/payload.json" `
  --cli-binary-format raw-in-base64-out `
  response.json
```

---

## Verificar S3 (datos presentes antes de lanzar el siguiente paso)

```powershell
# Listar lo que hay en staging para un cliente/marca/capa:
aws s3 ls s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/VISA/ --profile itx-dev

# Verificar que existe el parquet de un file_id concreto:
aws s3 ls "s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/VISA/400_baseii_cal_drafts/file_type=IN/date=2026-01-03/" --profile itx-dev
```

---

## Verificar tablas en Glue catalog

```powershell
aws glue get-tables `
  --profile itx-dev `
  --database-name itl_0004_itx_dev_02_glue_database_staging_ebgr_visa `
  --query "TableList[].{Name:Name,Updated:UpdateTime}" `
  --output table
```

---

## Sesión de debugging 2026-06-06 — lo que se ejecutó

**Job:** `glue-vi-calculate` para EBGR / VISA / IN / 2026-01-03

- `file_id`: `93BF199C85D2DF243AFDABEE5572E8C0`
- `content_hash`: `D44C4427AED04C1E078AA86B275060FA`
- `JobRunId`: `jr_3cebca36e4e90a00381cdf8bd0a3e578a69314bf7683e58de881a33bbed62033`
- Resultado: SUCCESS

**Crawler:** `itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa`
- Lanzado inmediatamente después del calculate
- Resultado: RUNNING al momento de guardar (pendiente confirmar SUCCEEDED)

**Archivos de soporte creados:**
- `tst_files/vi-calculate-run-test.txt` — argumentos del job en texto plano
- `tst_files/vi-calculate-run-args.json` — JSON generado para el CLI
- `tst_files/generate_glue_args.py` — script que convierte txt → json

---

## Sesión de debugging 2026-06-06 (cont.) — bug ARDEF en calculate, fix y re-deploy

**Hallazgo:** El `calculate.parquet` generado en la sesión anterior tenía los 10 campos derivados de ARDEF en 100% null (`ardef_country`, `product_id`, `funding_source`, `b2b_program_id`, `fast_funds`, `nnss_indicator`, `product_subtype`, `technology_indicator`, `travel_indicator`, `issuer_country`).

**Causa:** `load_visa_ardef()` parseaba `effective_date` (formato `yyyyMMdd`) con `F.to_date()` sin formato explícito → devolvía `NULL` para el 100% de las filas → ARDEF quedaba vacío tras el filtro de fechas → join sin matches. Detalle completo en `gotchas.md` → "glue-vi-calculate: load_visa_ardef() vaciaba el ARDEF...".

**Fix aplicado:** `F.to_date(F.col("effective_date"), "yyyyMMdd")` + eliminación de un pre-filtro de strings con formatos de fecha incompatibles.

### Subir el script corregido al S3 del Glue job

`sync-glue.ps1` solo descarga (AWS → repo). Para subir un script editado localmente de vuelta a AWS, usar `aws s3 cp` directo al `ScriptLocation` que figura en `glue/scripts/<marca>/<job>/config.json` (campo `Job.Command.ScriptLocation`):

```
s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py
```

```powershell
aws s3 cp `
  glue/scripts/visa/calculate/calculate.py `
  s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py `
  --profile itx-dev
```

El siguiente `start-job-run` usará automáticamente la versión recién subida — no requiere ningún paso adicional de "deploy" o invalidación de caché.

### Re-ejecutar el job con los mismos argumentos de la corrida anterior

```powershell
aws glue start-job-run `
  --profile itx-dev `
  --job-name itl-0004-itx-dev-intchg-02-glue-vi-calculate `
  --arguments "file://tst_files/vi-calculate-run-args.json"
```

**Resultado de esta sesión (2026-06-06):**
- `JobRunId`: `jr_a9f5bf312cfbf14dd2131d7e7ca275cf2f34e099e15a2e315e6cc291f8253e96`
- Resultado: **SUCCEEDED**

### Lanzar el crawler para refrescar el catálogo con el nuevo Parquet

```powershell
aws glue start-crawler `
  --profile itx-dev `
  --name itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa
```
(sin output = arrancó correctamente; lanzado tras confirmar el `calculate` en SUCCEEDED)

### Validar el fix

1. Descargar el nuevo `calculate.parquet` generado a `tst_files/` (sobrescribiendo el anterior)
2. Re-correr `python tst_files/debug_ardef_join.py` — el PASO 5 debe mostrar ~100% de match en los 10 campos ARDEF (antes: 0%, todo null)

---

## Sesión de debugging 2026-06-08 — bugs en glue-vi-interchange (content_hash perdido + acceptance_terminal_indicator "Space"), fix y subida a S3

**Hallazgo 1 — `content_hash` ausente en el Parquet ITX:** `evaluate_interchange_fees()` usa `mapInPandas()`, que reemplaza el schema completo del DataFrame; `content_hash` no estaba declarado en `OUTPUT_COLS` ni en `output_schema`, así que se descartaba silenciosamente aunque sí llegaba como columna de entrada (propagada desde clean/calculate vía `merged = cln_df.join(cal_df...)`). Detalle completo en `gotchas.md` → "glue-vi-interchange: content_hash se perdía en el Parquet ITX por mapInPandas".

**Hallazgo 2 — `acceptance_terminal_indicator` con criterio "Space" no matcheaba:** comparando `_apply_default()` (Glue) contra `_apply_condition_default()` (prototipo local en `tst_files/interchange_local.py`) se encontró un `value = value.strip()` extra que convertía el espacio literal `' '` en `''`, excluyendo transacciones GR con `acceptance_terminal_indicator=' '` de la regla `intelica_id=39` ("GR SECURE CR") y desviándolas a la regla fallback `63` ("GR NON-SEC CR"). Validado contra el operational `D44C4427AED04C1E078AA86B275060FA.parquet`: 524 transacciones GR cumplían TODAS las demás condiciones de la regla 39 y fueron mal clasificadas solo por este bug. Detalle completo en `gotchas.md` → "glue-vi-interchange: _apply_default() destruía el token Space".

**Fixes aplicados (2026-06-08) en `glue/scripts/visa/interchange/interchange.py`:**
1. `"content_hash"` agregado como primer elemento de `OUTPUT_COLS` y `StructField("content_hash", StringType(), True)` como primer campo de `output_schema` (función `evaluate_interchange_fees`)
2. Eliminado el `value = value.strip()` extra dentro del loop de `_apply_default()` (línea ~300)

### Subir el script corregido al S3 del Glue job

Mismo patrón que la sesión del fix de ARDEF (`calculate.py`, sección anterior). `ScriptLocation` del job `glue-vi-interchange` (campo `Job.Command.ScriptLocation` en `glue/scripts/visa/interchange/config.json`):

```
s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py
```

```powershell
aws s3 cp `
  glue/scripts/visa/interchange/interchange.py `
  s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py `
  --profile itx-dev
```

**Resultado de esta sesión (2026-06-08):** subida completada —
`upload: glue\scripts\visa\interchange\interchange.py to s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py`

El siguiente `start-job-run` de `itl-0004-itx-dev-intchg-02-glue-vi-interchange` usará automáticamente esta versión.

### Pendiente (próxima sesión)

1. Re-ejecutar `glue-vi-interchange` (mismo patrón txt→json→start-job-run que en `vi-calculate`, ver Pasos 1-4 al inicio de este documento) para el `file_id`/`content_hash` `D44C4427AED04C1E078AA86B275060FA`
2. Descargar el nuevo `itx.parquet` y validar:
   - `content_hash` aparece como **primera columna**
   - Las transacciones GR con `acceptance_terminal_indicator=' '` que cumplen el resto de condiciones de la regla 39 ahora obtienen `interchange_intelica_id=39` (`GR SECURE CR`) en vez de `63` (`GR NON-SEC CR`)

---

## Sesión de debugging 2026-06-08 — bug _parse_dates en lmbd-vi-clean (fechas YDDD/MMDD incorrectas)

**Hallazgo:** Tres de los cuatro campos de fecha en `clean.parquet` producían valores incorrectos — raíz en la lógica "compute-then-correct" de `_parse_dates()`.

| Campo | Bug | Ejemplo incorrecto | Correcto |
|-------|-----|--------------------|---------|
| `central_processing_date` | `!YDDD` restaba 10 años si resultado > file_date | `2016-01-04` | `2026-01-04` |
| `account_reference_number_date` | Mismo bug | `2016-01-04` | `2026-01-04` |
| `purchase_date` | `!MMDD` comparaba fecha completa vs solo mes | `2025-01-04` | `2026-01-04` |
| `conversion_date` | `!YDDD` sin cap → fecha futura sin corrección | `2026-01-04` | `2025-01-04` |

**Debugging:** Comparación de conteos agrupados por fecha contra PostgreSQL legacy usando `tst_files/debug_clean_dates.py` (sobre `tst_files/extract.parquet`, file_date=2026-01-03). Se leyó spec Visa (`tst_files/fechas.txt`) y adapters.py del sistema legacy para derivar la lógica correcta.

**Fix aplicado en `lambdas/visa/clean/src/handler.py`:** Reescritura completa de `_parse_dates()`:
- `!YDDD` → `decade_of(file_date) + Y + DDD`, sin corrección posterior
- `!YDDD_MAX` → igual que `!YDDD` + cap: si resultado > `file_date` → restar 1 año
- `!MMDD` → inferir año comparando solo el mes (`src_month > reference_date.month`)
- Todos los formatos: `'0000'` → `file_date`

**DynamoDB actualizado:**
```powershell
aws dynamodb update-item `
  --profile itx-dev `
  --table-name itl-0004-itx-dev-dynamo-visa_fields-02 `
  --key '{"type_record": {"S": "draft"}, "column_name": {"S": "conversion_date"}}' `
  --update-expression "SET date_format = :v" `
  --expression-attribute-values '{":v": {"S": "!YDDD_MAX"}}' `
  --return-values ALL_NEW
```

Nota: las claves reales de `visa_fields-02` son `type_record` (HASH) + `column_name` (RANGE).

**Resultado de esta sesión (2026-06-08):** handler.py subido al Lambda `lmbd-vi-clean` por el usuario — pendiente confirmar resultado en producción.
