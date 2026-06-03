# Creación Manual de Databases y Crawlers en AWS Glue
**Proyecto:** ITX Data Lake | **Ambiente:** dev | **Región:** eu-south-2

---

## Objetos a crear (16 en total)

| # | Tipo | Nombre |
|---|------|--------|
| 1 | Database | itl_0004_itx_dev_intchg_02_glue_database_operational_ebgr_visa |
| 2 | Database | itl_0004_itx_dev_intchg_02_glue_database_operational_ebgr_mc |
| 3 | Database | itl_0004_itx_dev_intchg_02_glue_database_operational_sbsa_visa |
| 4 | Database | itl_0004_itx_dev_intchg_02_glue_database_operational_sbsa_mc |
| 5 | Database | itl_0004_itx_dev_intchg_02_glue_database_staging_ebgr_visa |
| 6 | Database | itl_0004_itx_dev_intchg_02_glue_database_staging_ebgr_mc |
| 7 | Database | itl_0004_itx_dev_intchg_02_glue_database_staging_sbsa_visa |
| 8 | Database | itl_0004_itx_dev_intchg_02_glue_database_staging_sbsa_mc |
| 9 | Crawler | itl_0004_itx_dev_intchg_02_glue_crawler_operational_ebgr_visa |
| 10 | Crawler | itl_0004_itx_dev_intchg_02_glue_crawler_operational_ebgr_mc |
| 11 | Crawler | itl_0004_itx_dev_intchg_02_glue_crawler_operational_sbsa_visa |
| 12 | Crawler | itl_0004_itx_dev_intchg_02_glue_crawler_operational_sbsa_mc |
| 13 | Crawler | itl_0004_itx_dev_intchg_02_glue_crawler_staging_ebgr_visa |
| 14 | Crawler | itl_0004_itx_dev_intchg_02_glue_crawler_staging_ebgr_mc |
| 15 | Crawler | itl_0004_itx_dev_intchg_02_glue_crawler_staging_sbsa_visa |
| 16 | Crawler | itl_0004_itx_dev_intchg_02_glue_crawler_staging_sbsa_mc |

---

## Paso 1 — Databases

**Ruta:** Glue → Data Catalog → Databases → Add database

Crear las 8 databases con solo **Name** y **Description**. El resto en blanco.

| Database (sufijo) | Description |
|-------------------|-------------|
| `...operational_ebgr_visa` | Datos finales VISA cliente EBGR — salida de lmbd-vi-store |
| `...operational_ebgr_mc` | Datos finales Mastercard cliente EBGR — salida de lmbd-mc-store |
| `...operational_sbsa_visa` | Datos finales VISA cliente SBSA — salida de lmbd-vi-store |
| `...operational_sbsa_mc` | Datos finales Mastercard cliente SBSA — salida de lmbd-mc-store |
| `...staging_ebgr_visa` | Etapas intermedias VISA cliente EBGR (RAW → EXT → CLN → CAL → ITX) |
| `...staging_ebgr_mc` | Etapas intermedias Mastercard cliente EBGR (RAW → TRA → EXT → CLN → CAL → ITX) |
| `...staging_sbsa_visa` | Etapas intermedias VISA cliente SBSA (RAW → EXT → CLN → CAL → ITX) |
| `...staging_sbsa_mc` | Etapas intermedias Mastercard cliente SBSA (RAW → TRA → EXT → CLN → CAL → ITX) |

---

## Paso 2 — Crawlers

**Ruta:** Glue → Data Catalog → Crawlers → Create crawler

Repetir para los 8 crawlers con los valores de las tablas siguientes.

### Configuración común a todos los crawlers

| Campo | Valor |
|-------|-------|
| IAM role | `AWSGlueServiceRole-glue-crawler-s3-staging-role` (ya existe) |
| Frequency | On demand |
| Schema change — Update | Update the table definition in the data catalog |
| Schema change — Delete | Mark the table as deprecated in the data catalog |
| Table name prefix | Dejar en blanco |

### Valores específicos por crawler

| Crawler (sufijo) | S3 Path | Database |
|------------------|---------|----------|
| `...operational_ebgr_visa` | `s3://itl-0004-itx-dev-intchg-02-s3-operational/EBGR/VISA/` | `...database_operational_ebgr_visa` |
| `...operational_ebgr_mc` | `s3://itl-0004-itx-dev-intchg-02-s3-operational/EBGR/MC/` | `...database_operational_ebgr_mc` |
| `...operational_sbsa_visa` | `s3://itl-0004-itx-dev-intchg-02-s3-operational/SBSA/VISA/` | `...database_operational_sbsa_visa` |
| `...operational_sbsa_mc` | `s3://itl-0004-itx-dev-intchg-02-s3-operational/SBSA/MC/` | `...database_operational_sbsa_mc` |
| `...staging_ebgr_visa` | `s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/VISA/` | `...database_staging_ebgr_visa` |
| `...staging_ebgr_mc` | `s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/MC/` | `...database_staging_ebgr_mc` |
| `...staging_sbsa_visa` | `s3://itl-0004-itx-dev-intchg-02-s3-staging/SBSA/VISA/` | `...database_staging_sbsa_visa` |
| `...staging_sbsa_mc` | `s3://itl-0004-itx-dev-intchg-02-s3-staging/SBSA/MC/` | `...database_staging_sbsa_mc` |

### ⚠️ Configuración avanzada — Table level (CRÍTICO)

En la sección **Advanced options** de cada crawler configurar:

| Campo | Valor |
|-------|-------|
| Table grouping policy | Combine compatible schemas |
| **Table level** | **4** |
| Create partition indexes | Activado |

**Por qué nivel 4:** La estructura de los buckets es `{bucket}/{client}/{brand}/{dataset}/file_type=x/date=x/`. El nivel 4 (contando desde la raíz del bucket) crea una tabla por carpeta de dataset. Los niveles `file_type=` y `date=` quedan por debajo y son detectados automáticamente como partition keys.

```
s3://...bucket/        ← nivel 1
    EBGR/              ← nivel 2
        VISA/          ← nivel 3
            baseii_drafts/  ← nivel 4 → UNA TABLA AQUÍ
                file_type=IN/    ← partition key automática
                    date=2026-06-03/  ← partition key automática
```

> Si se deja el nivel por defecto el crawler mezclará todos los datasets en una sola tabla.

---

## Paso 3 — Ejecutar los crawlers

Glue → Crawlers → seleccionar todos → **Run**

Se pueden ejecutar en paralelo. Tardan ~2-5 min cada uno. Estado esperado al finalizar: **Ready / Succeeded**.

> Las tablas solo aparecen si ya existen archivos Parquet en el path. Si un path está vacío el crawler no crea tabla — esto es normal.
