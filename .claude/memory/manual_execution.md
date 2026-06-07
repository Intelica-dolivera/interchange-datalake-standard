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
