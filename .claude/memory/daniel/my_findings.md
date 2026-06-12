---
name: daniel-findings
description: Hallazgos propios de Daniel — bugs, decisiones y gotchas descubiertos en su trabajo
metadata:
  type: feedback
---

# Hallazgos de Daniel

Problemas y decisiones descubiertos durante el desarrollo de los módulos propios de Daniel.
Formato idéntico a `.claude/memory/gotchas.md` de Julio para facilitar eventual PR hacia upstream.

---

## MC pipeline: file_id / content_hash / file_processing_date ausentes en MTI 1644 y 1740 — RESUELTO (pendiente validar en AWS)

**Archivos afectados:**
- `lambdas/mastercard/interpreter/src/handler.py`
- `lambdas/mastercard/transform/src/handler.py`
- `lambdas/mastercard/extract/src/handler.py`
- `lambdas/mastercard/clean/src/handler.py`

**Detectado:** 2026-06-10

**Síntoma:** `file_id` presente en CLN para MTI 1240 y 1442, pero ausente en MTI 1644 y 1740.

**Causa raíz (por capa):**

| Capa | MTI 1240/1442 | MTI 1644 | MTI 1740 |
|------|--------------|----------|----------|
| **Interpreter (RAW)** | ❌ no inyectaba ninguno | ❌ | ❌ |
| **Transform (TRA)** | ✅ inyectaba `file_id` + `file_processing_date` + `content_hash` | ❌ solo `content_hash` | ❌ solo `content_hash` |
| **Extract (EXT)** | ✅ `_reorder_cols` preserva extras | ❌ `_align_df_1644` descartaba todo con `return df[wanted]` | ✅ `_reorder_cols` preserva extras (pero no venía nada del TRA) |
| **Clean (CLN)** | ✅ `with_file_cols=True` + extras | ❌ re-inyectaba solo `content_hash` desde event | ❌ mismo |

**Bug adicional en interpreter:** `content_hash` se asignaba a `df_block` pero `_ensure_and_cast` lo descartaba silenciosamente porque no estaba en el schema canónico.

**Solución aplicada (2026-06-10):**

1. **mc-interpreter** — `_canonical_schema_from_de_spec`: agregados `pa.field("file_id", pa.string())` y `pa.field("content_hash", pa.string())`. En `_process_block`: agregado `df_block["file_id"] = file_id`. Commit `32f8deb`.

2. **mc-transform** — `transform_ipm_1644`: extraído `file_processing_date = file_config["file_processing_date"]` y agregado a df_685/688/691 junto a `file_id`. Mismo para `transform_ipm_1740`. Commit `6fa222e`.

3. **mc-extract** — `_align_df_1644`: cambiado `return df[wanted]` por `extras = [c for c in df.columns if c not in set(wanted)]; return df[wanted + extras]` (mismo patrón que `_reorder_cols`). Eliminadas re-inyecciones de `content_hash` desde event en `_extract_1644` y `_extract_standard`. Commit `e889c2f`.

4. **mc-clean** — Eliminadas re-inyecciones de `content_hash` desde event en `_clean_1644` y `_clean_standard`. `_cast_df` ya preserva extras al final. Commit `7f54693`.

**Decisión de diseño — propagación via extras (no re-inyección desde event):**
Se eligió que los tres campos fluyan como extras a través del pipeline (RAW → TRA → EXT → CLN) en vez de re-inyectarlos en cada etapa desde el event. Motivo: ambiente de prueba donde cada run hace borron y cuenta nueva de todos los parquets S3, por lo que la fragilidad del enfoque no es un problema por ahora.

**Validado (2026-06-11):** prueba con archivo único — los CLN de MTI 1240, 1644 (FC 685/688) y 1740 contienen correctamente `file_id`, `file_processing_date` y `content_hash` en todos los parquets.

**Si vuelve a aparecer (algún MTI sin esos campos en CLN):**
1. Verificar que el RAW tiene las 3 columnas en su schema Parquet
2. Verificar que el TRA las inyecta (`file_id` + `file_processing_date` + `content_hash`)
3. Verificar que el EXT no las descartó (para 1644: `_align_df_1644` debe tener el bloque `extras`)
4. Verificar que el CLN no las sobreescribió (buscar `df_cast["content_hash"] =` en handler.py)

---

## OOM en mc-interpreter, mc-extract, mc-clean: materialización completa del DataFrame — FIXES APLICADOS (pendiente validar mc-clean)

**Archivos afectados:**
- `lambdas/mastercard/interpreter/src/handler.py`
- `lambdas/mastercard/extract/src/handler.py`
- `lambdas/mastercard/clean/src/handler.py`

**Detectado:** 2026-06-11

**Síntoma:** `Runtime.OutOfMemory` (10240 MB) en los tres lambdas. En mc-interpreter y mc-extract crasheaban en ~22s.

**Causa raíz común:** materialización del DataFrame completo en RAM antes de cualquier procesamiento.

### mc-interpreter (`_process_block`)

`_process_block` construía `wide_rows = [build_wide_row(...) for row in block_buffer]` para todos los mensajes del bloque (400K+) de una sola vez, luego `pd.DataFrame(wide_rows)` — pico de ~10 GB.

**Fix (2026-06-11):** chunking dentro de `_process_block` con `chunk_size = ITX_INTERPRETER_BLOCK_CHUNK_SIZE` (default 10,000). Por cada sub-chunk: `build_wide_row` → `pd.DataFrame` → `write_parquet_by_mti_block_streaming` → `del df_chunk; gc.collect(); pa.default_memory_pool().release_unused()`. El `block_buffer` (lista de dicts Python) se acumula completo (seguro — no son bytes, no puede partir un mensaje entre chunks).

**Resultado:** de OOM (10240 MB) a 4473 MB. ✓

### mc-extract (`_extract_standard`, `_extract_1644`)

`_read_parquet(key)` → `Body.read()` + `pd.read_parquet(BytesIO(body))` materializaba el DataFrame completo (~3 GB). No tiene filtro de columnas que reduzca el tamaño antes de procesar (a diferencia de mc-transform que descarta columnas con `filter_df_columns_de`). EphemeralStorage 512 MB — no puede usar /tmp para archivos grandes.

**Fix (2026-06-11):** reemplazado el patrón de lectura completa por `iter_batches`:
```python
body = S3.get_object(...)["Body"].read()
in_buf = io.BytesIO(body); del body; gc.collect()
pf = pq.ParquetFile(in_buf)
out_buf = io.BytesIO(); writer = None
for batch in pf.iter_batches(batch_size=EXTRACT_BATCH_SIZE):   # 100K filas por defecto
    df = batch.to_pandas()
    # ... transforms (rename, fill, reorder) ...
    table = pa.Table.from_pandas(df, preserve_index=False)
    if writer is None: writer = pq.ParquetWriter(out_buf, table.schema, compression="snappy")
    writer.write_table(table); del df, table; gc.collect()
writer.close()
out_buf.seek(0); S3.put_object(Body=out_buf)
```
`cols_to_drop` y `missing` se calculan solo en el primer batch (los nombres de columna son iguales en todos los batches del mismo archivo).
Nueva constante: `EXTRACT_BATCH_SIZE = int(os.environ.get("ITX_EXTRACT_BATCH_SIZE", "100000"))`.
Imports agregados: `import pyarrow as pa; import pyarrow.parquet as pq`.

**Pico de RAM estimado:** N_comprimido (in_buf) + 100K filas descomprimidas ≈ 600–800 MB vs ~3.5 GB antes.

**Resultado:** funcionó sin OOM. ✓

### mc-clean (`_clean_standard`, `_clean_1644`)

Mismo patrón que mc-extract. Complicación adicional: `_write_parquet_with_schema` aplica coerción de tipos Arrow (string/int64/int32/date) antes de escribir. Esta lógica fue extraída al nuevo helper `_align_df_to_schema(df, schema) → pa.Table` para usarla por batch.

**Fix (2026-06-11):** mismo patrón `iter_batches` + `pq.ParquetWriter`. El schema Arrow se construye una vez del primer batch y se reutiliza (para `_clean_standard`: en todos los archivos del MTI; para `_clean_1644`: por archivo, porque cada FC produce columnas distintas).
Nueva constante: `CLEAN_BATCH_SIZE = int(os.environ.get("ITX_CLEAN_BATCH_SIZE", "100000"))`.
Nuevo helper `_align_df_to_schema(df, schema) → pa.Table` (extrae la coerción de tipos de `_write_parquet_with_schema`).
`_write_parquet_with_schema` simplificada: delega en `_align_df_to_schema` y usa `buf.seek(0)` + `put_object(Body=buf)` (elimina `buf.getvalue()` que creaba copia extra).

**Estado:** validado en AWS (2026-06-12). ✓

**Si vuelve a aparecer OOM en cualquiera de estos lambdas después del fix:** verificar que `ITX_*_BATCH_SIZE` no esté en un valor demasiado alto. Reducir a 50,000 si el archivo fuente tiene columnas muy anchas (300+ cols con strings largos). El pico por batch es aproximadamente `batch_size × n_cols × avg_bytes_per_col`.

---

## MC transform MTI 1240: content_hash silenciosamente eliminado por align_chunk_to_expected_columns — RESUELTO

**Archivo:** `lambdas/mastercard/transform/src/handler.py` (función `build_expected_columns`)
**Detectado:** 2026-06-10

**Síntoma:** El TRA del MTI 1240 no tenía `content_hash` en su Parquet de salida, pese a que los TRA de MTI 1442, 1644 y 1740 sí lo tenían. `file_id` y `file_processing_date` sí aparecían correctamente.

**Causa raíz:** `transform_ipm_1240` es el único MTI que usa `align_chunk_to_expected_columns` (chunking dinámico). El flujo:
1. `expected_columns = build_expected_columns(...)` — construía la lista sin `"content_hash"`
2. `chunk["content_hash"] = content_hash` — se agregaba correctamente
3. `chunk = align_chunk_to_expected_columns(chunk, expected_columns)` — la línea `chunk = chunk[expected_columns]` filtraba el DataFrame a exactamente esas columnas, eliminando silenciosamente `content_hash`

`build_expected_columns` terminaba con `cols.extend(["file_processing_date", "file_id"])` pero no incluía `"content_hash"`. Los otros MTIs (1442, 1740) asignan `content_hash` y escriben directamente sin pasar por `align_chunk_to_expected_columns`, por eso no tenían el problema.

**Solución aplicada (2026-06-10):** `"content_hash"` agregado a la lista en `build_expected_columns`:
```python
cols.extend([
    "file_processing_date",
    "file_id",
    "content_hash",   # ← agregado
])
```
Validado con un archivo real procesado end-to-end. Commit `7f1bd4a`.

---

## mc-clean _align_df_to_schema: KeyError cuando columna del schema no existe en el parquet — RESUELTO

**Archivo:** `lambdas/mastercard/clean/src/handler.py` (función `_align_df_to_schema`)
**Detectado:** 2026-06-12

**Síntoma:** `KeyError: "name 'electronic_commerce_indicator_1_pds_52_1' present in the specified schema is not found in the columns or index"` al procesar el segundo parquet de MTI 1442. El primero pasaba OK, el tercero nunca llegó a intentarse.

**Causa raíz:** El schema Arrow se construye una sola vez del primer parquet del MTI y se reutiliza para todos los siguientes. Cuando el segundo parquet tiene `electronic_commerce_indicator_pds_52` (campo padre) null en todas sus filas, el TRA/EXT no generó las columnas subfield (`_1_pds_52_1`, etc.). `_align_df_to_schema` filtraba esas columnas ausentes del df correctamente:
```python
df_aligned = df[[c for c in schema_cols if c in df.columns]].copy()
```
Pero luego llamaba `pa.Table.from_pandas(df_aligned, schema=schema)` con el schema completo que sí las exigía → PyArrow lanzaba `KeyError`.

**Solución aplicada (2026-06-12):** En el loop de coerción de tipos, cuando una columna del schema no está en `df_aligned`, en vez de `continue` se asigna `None`:
```python
if col not in df_aligned.columns:
    df_aligned[col] = None   # columna ausente → null, schema consistente
    continue
```
PyArrow con schema explícito convierte la columna de `None`s al tipo correcto (string null, int null, date null).

**Por qué es el fix correcto (no reconstruir schema por parquet):** reconstruir el schema por parquet produciría CLN con columnas distintas entre archivos del mismo MTI, rompiendo los reads downstream (Glue/Athena). El output siempre debe tener el mismo schema; las columnas ausentes en un chunk son simplemente null en ese chunk.

**Si vuelve a aparecer (`KeyError` en `pa.Table.from_pandas` con schema explícito):** el patrón es siempre el mismo — schema tiene más columnas que el df. Verificar que `_align_df_to_schema` asigne `None` para las columnas faltantes antes del `from_pandas`.

---

## mc-store: OOM (10240 MB) por copias innecesarias de DataFrames y buf.getvalue() — RESUELTO

**Archivo:** `lambdas/mastercard/store/src/handler.py`
**Detectado:** 2026-06-12

**Síntoma:** `Runtime.OutOfMemory` en el Lambda mc-store procesando archivo SBSA (file_id `E98AEBFCDDD92A013254E148BE81516F`, 32 parquets CLN entre MTIs 1240/1442/1644/1740).

**Causa raíz — cuatro fuentes de desperdicio de memoria:**

1. **`_normalize_merge_keys` hacía `df = df.copy()`** — copiaba el DataFrame completo (cientos de columnas) para normalizar solo 3 columnas de llave. Para un CLN 1240 de ~247 MB comprimido → ~2–3 GB descomprimido, esta copia duplicaba el pico en memoria.

2. **`merged = df_cln.copy()`** — otra copia completa del CLN sin ninguna razón. `df_cln` nunca se usaba después de esta asignación.

3. **`gc.collect()` ausente después de liberar CAL e ITX** — Python no reclamaba la memoria de los DataFrames anteriores antes de iniciar el siguiente merge.

4. **`_write_parquet_s3`: `S3.put_object(Body=buf.getvalue())`** — `getvalue()` crea una segunda copia de los bytes ya serializados en el BytesIO. Para un parquet de 50 MB esto significa 50 MB extra en el pico de escritura.

**Solución aplicada (2026-06-12):**

| Fix | Código |
|-----|--------|
| `_normalize_merge_keys`: modifica in-place | Eliminado `df = df.copy()` — `df[k] = ...` muta el objeto pasado por referencia |
| Sin copia del CLN | `merged = df_cln` + `del df_cln` + `gc.collect()` |
| Liberar CAL/ITX inmediatamente | `del df_cal; gc.collect()` y `del df_itx; gc.collect()` tras cada merge |
| Write sin duplicar bytes | `buf.seek(0)` + `S3.put_object(Body=buf)` en vez de `buf.getvalue()` |

**Peak de memoria con el fix:** durante el merge `merged.merge(df_cal[KEYS + new_cols])` existen simultáneamente `merged_anterior + df_cal_subset + merged_nuevo` ≈ 2× el tamaño del CLN. Este es el mínimo inevitable con pandas merge key-based — no se puede reducir sin cambiar la estrategia de merge.

**Nota sobre el key-based merge:** se mantiene intencionalmente (vs positional concat) porque es más seguro ante reordenamientos de filas en etapas upstream. Las llaves son `["file_id", "file_idn", "ref_id"]`.

**Validación (2026-06-12):** invocación directa del Lambda via `aws lambda invoke --invocation-type Event` con payload construido desde los CLN en staging (32 outputs: 12×1240, 3×1442, 14×1644, 3×1740). Confirmado SUCCESS en CloudWatch — sin OOM. Payload de referencia en `tst_files/mc-store-payload.json`.

**Si vuelve a aparecer OOM en mc-store:** el pico irreducible es ~2× el CLN más grande del lote. Para el archivo SBSA el CLN más grande es `..._0012601130000000126401151_1240.parquet` (247 MB comprimido → ~1.5–2 GB descomprimido). Si eso no cabe en 10 GB con el merge, la única salida es procesar los outputs del store de a uno por invocación (Step Functions paraleliza).

---

## MC transform: list_parquet_files no filtraba por file_id — cross-contamination en ejecuciones paralelas — RESUELTO

**Archivo:** `lambdas/mastercard/transform/src/handler.py` (método `FileStorage.list_parquet_files`)
**Detectado:** 2026-06-11

**Síntoma:** Al correr varios archivos del mismo cliente en paralelo vía Step Functions, el TRA de un archivo tenía el `content_hash` y `file_id` de otro archivo que corría en paralelo. A partir de EXT los valores se veían correctos (por la race condition descrita abajo).

**Causa raíz:** `list_parquet_files` construía el prefijo S3 como `{client_id}/MC/{subdir}/file_type={file_type}/date={processing_date}/` y devolvía **todos** los parquets bajo ese prefijo sin filtrar por `file_id`. Cuando dos archivos del mismo cliente/file_type/fecha corrían en paralelo, cada invocación del transform listaba los RAWs de ambos archivos y los procesaba, estampando todos con su propio `file_id`/`content_hash`.

El interpreter nombra los RAW parquets como `{file_id}_{file_idn}_{mti}.parquet`, por lo que el filtro correcto es `name.startswith(file_id)` — exactamente el mismo que ya usaban extract (`_list_parquet_keys`, línea 467) y clean (`_list_parquet_keys`, línea 715).

**Por qué EXT/CLN parecían correctos:** race condition — el transform del archivo B, al correr después del transform A para el mismo parquet de B, sobreescribía el TRA con los valores correctos de B. Si el extract corría después de eso, encontraba valores correctos. No era una corrección real sino un resultado no determinístico del orden de ejecución.

**Solución aplicada (2026-06-11):**
```python
# ANTES (bug): devolvía todos los parquets del prefijo de fecha
if key.endswith(".parquet"):
    keys.append(key)

# DESPUÉS (correcto): solo los parquets del file_id actual
name = key.rsplit("/", 1)[-1]
if name.startswith(file_id) and name.endswith(".parquet"):
    keys.append(key)
```
Commit `2556480`. Validado con múltiples archivos en paralelo — ya no hay cross-contamination.

**Si vuelve a aparecer (TRA con file_id/content_hash de otro archivo en paralelo):** verificar que `list_parquet_files` en transform tenga el filtro `name.startswith(file_id)`. El patrón debe ser idéntico en las tres capas (transform, extract, clean).
