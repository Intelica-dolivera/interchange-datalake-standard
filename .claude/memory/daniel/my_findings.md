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
