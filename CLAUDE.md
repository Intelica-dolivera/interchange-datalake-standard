# CLAUDE.md — ITX AWS Pipeline (interchange-datalake-aws)

## Contexto del proyecto

Este repositorio es la **migración a AWS** de un sistema de procesamiento de archivos de interchange de las marcas Visa y Mastercard.

**Sistema origen (legacy):** AWS con RDS Aurora PostgreSQL + EC2 + S3 + SQS + Lambda.

**Sistema destino (este proyecto):** Arquitectura Data Lake en AWS — Lambda + Step Functions + S3 + DynamoDB + Glue + Athena. Objetivos: mejor paralelismo, menor tiempo de procesamiento y reducción de costos.

**Flujo de desarrollo:**
1. Se validó la lógica de negocio en un prototipo local (carpetas + archivos .parquet).
2. Este repo adapta esa lógica validada a los servicios AWS correspondientes.
3. El repositorio crece incrementalmente — se añaden componentes a medida que se validan.

**Organización:** Intelica IT
**Desarrollador:** Julio Cesar Cardenas Suca
**Runtime:** Python 3.11
**Región AWS:** eu-south-2 (cuenta de prueba)
**Estado:** Pipeline Visa implementado y validado. Mastercard: todos los componentes implementados (2026-06-02) — en validacion end-to-end con itx-mastercard-orchestrator.

---

## Flujo de datos

### Diagrama general

```
S3 Landing  {client_id}/{filename}
    ↓  [S3 Event s3:ObjectCreated:*]
lmbd-router
    ↓  (1) extrae client_id del path
    ↓  (2) si ZIP → lmbd-unzip [async, no espera] → cada archivo extraído
    ↓       dispara el router nuevamente via S3 event → paralelismo gratis
    ↓  (3) clasifica via DynamoDB file_pattern (regex por prioridad)
    ↓  (4) extrae fecha del header: Visa → primeros 50 bytes; MC → chunks buscando trailer 1644/695
    ↓  (5) calcula MD5 en streaming → detecta duplicados en file_control
    ↓       mismo MD5        → SKIPPED (ya procesado)
    ↓       mismo nombre/distinto MD5 → nuevo file_id (nueva versión)
    ↓  (6) registra en DynamoDB file_control con estado PENDING → PROCESSING
    ↓
    ├── direction=ARDEF ──→ lmbd-vi-ardef     [async directo, sin Step Function]
    ├── direction=IAR   ──→ lmbd-mc-iar       [async directo, sin Step Function]
    ├── brand=VISA      ──→ itx-visa-orchestrator (Step Function)
    └── brand=MASTERCARD──→ itx-mastercard-orchestrator (Step Function)

Step Functions (flujo normal IN/OUT)
    ↓
lmbd-{marca}-transform      → parsea archivo → Parquet           → S3 Staging
                               (Visa: texto plano latin-1, ancho fijo)
                               (MC:   EBCDIC)
    ↓
lmbd-{marca}-extract        → mapeo de campos                    → S3 Staging/extracted/
    ↓
lmbd-{marca}-clean          → validacion y limpieza              → S3 Staging/clean/
    ↓
glue-{marca}-calculate      → calculo de fees por transaccion
    ↓
glue-{marca}-interchange    → reporte consolidado de interchange
    ↓
lmbd-{marca}-store          → Parquets finales → S3 Operational
    ↓
lmbd-archive-file           → archiva el archivo original        → S3 Archive
    ↓
Athena                      → consultas SQL sobre datos finales
```

### Despacho por tipo de archivo

| direction | brand | Destino | Razon |
|-----------|-------|---------|-------|
| IN / OUT | VISA | `itx-visa-orchestrator` | Flujo normal de transacciones |
| IN / OUT | MASTERCARD | `itx-mastercard-orchestrator` | Flujo normal de transacciones |
| ARDEF | VISA | `lmbd-vi-ardef` (directo) | Archivo de reglas/BINes, no requiere orquestacion |
| IAR | MASTERCARD | `lmbd-mc-iar` (directo) | Archivo de reglas/BINes, no requiere orquestacion |
| ZIP | cualquiera | `lmbd-unzip` (async) | Se descomprime primero; cada archivo interno re-dispara el router |

**Por que ARDEF/IAR no usan Step Functions:** son archivos de reglas y rangos de BINes — procesos relativamente livianos comparados con los archivos transaccionales. Los archivos IN/OUT requieren Step Functions porque su procesamiento se divide en multiples etapas pesadas que superarian el timeout de un solo Lambda (max 900s).

---

### Flujo detallado — Visa (IN/OUT)

Los archivos Visa son **texto plano de ancho fijo, encoding latin-1**. Los campos se ubican por posicion segun los manuales de Visa.

**1. Transform** (`lmbd-vi-transform`)
Agrupa los bytes del archivo en records estructurados segun los manuales. Produce tres tipos:
- **BASE II / SMS** — records transaccionales
- **VSS** — records de liquidacion (lo que Visa reporta que cobro)
Genera Parquet en `s3-staging/transform/`.

**2. Extract** (`lmbd-vi-extract`)
Extrae campo por campo de cada record usando posiciones fijas definidas en los manuales.
Genera Parquet en `s3-staging/extract/`.

**3. Clean** (`lmbd-vi-clean`)
Con ayuda de la tabla `visa_fields` en DynamoDB, normaliza y formatea campos.
Formatos de fecha soportados: `!YDDD` (central_processing_date, account_reference_number_date — digito de año + julian day, sin corrección posterior), `!YDDD_MAX` (conversion_date — igual que `!YDDD` pero con cap en file_date, ya que una tasa futura es imposible), `!MMDD` (purchase_date — año inferido por comparación de mes), `!YYYYDDD`. Todos mapean `'0000'` → `file_date`.
Genera Parquet en `s3-staging/clean/`. Este es el Parquet con los campos originales en su forma final correcta.

**4. Calculate** (`glue-vi-calculate`)
Calcula campos derivados necesarios para la tarificacion: tipo de transaccion, ciclo, pais del emisor (cruzando con data ARDEF), jurisdiccion, entre otros.
Genera Parquet en `s3-staging/calculate/`.
Se usa Glue (no Lambda) por la complejidad de las logicas y el volumen de datos.

**5. Interchange** (`glue-vi-interchange`)
Asigna la tarifa de interchange correcta a cada transaccion evaluando condiciones y logicas de clasificacion. El resultado se contrasta contra los records VSS para el Data Quality — validar que la tarificacion propia coincida con lo que Visa liquido.
Genera Parquet en `s3-staging/interchange/`.

**6. Store** (`lmbd-vi-store`)
Une los tres Parquets clave en un solo archivo consolidado:
- `clean` — campos originales normalizados
- `calculate` — campos derivados
- `interchange` — tarifas asignadas
Escribe el Parquet final a `s3-operational`. Sin este paso no existe el archivo que Athena consultara via crawler.

**7. Archive** (`lmbd-archive-file`)
Mueve el archivo original del landing a `s3-archive`.

---

### Flujo detallado — Mastercard (IN/OUT)

Los archivos Mastercard usan formato **IPM (ISO-8583)** con mensajes delimitados por RDW. El encoding puede ser `latin-1` o `cp500` (EBCDIC) — configurado por cliente en la tabla `client` de DynamoDB (`file_mc_encoding_in` / `file_mc_encoding_out`).

A diferencia de Visa, el flujo MC tiene un paso inicial exclusivo (`interpreter`) que no existe en Visa, necesario por la complejidad del formato IPM.

**1. Interpreter** (`lmbd-mc-interpreter`) — exclusivo Mastercard
Traduce el archivo IPM binario a Parquets estructurados por MTI. Pasos internos:
- Consulta DynamoDB para determinar encoding y si el archivo viene bloqueado (`file_block`) o requiere `interpreter_fix`
- Si bloqueado: elimina los 2 bytes de separador de cada bloque de 1014 bytes (`unblock_1014`)
- Lee mensaje a mensaje usando el RDW (4 bytes big-endian que indican el largo de cada mensaje)
- Parsea estructura ISO-8583: MTI (4 bytes) + bitmap (8 o 16 bytes) + Data Elements
- Agrupa mensajes en bloques delimitados por headers MTI 1644/FC 697 y trailers MTI 1644/FC 695
- Genera Parquets por MTI en `staging/100_IPM_{MTI}_RAW/`

MTIs que produce el interpreter:

| MTI | Contenido | Equivalente Visa |
|-----|-----------|-----------------|
| 1240 | Mensajes transaccionales principales | BASE II / SMS |
| 1442 | Mensajes transaccionales secundarios | BASE II / SMS |
| 1740 | Mensajes de fee collection | BASE II / SMS |
| 1644 (FC 685/688/691) | Mensajes de liquidacion de la marca | VSS |

**2. Transform** (`lmbd-mc-transform`)
Procesa los Parquets RAW del interpreter por MTI. No es extraccion de campos de negocio — es una etapa de preparacion y estructuracion previa al extract. Para cada MTI:
- Carga layout de DEs y PDS desde DynamoDB (`mastercard_fields`) — configuration-driven
- Filtra columnas DE relevantes segun layout
- Expande subcampos de ancho fijo y reordena columnas (subcampos junto a su DE padre)
- Extrae y expande PDS (Private Data Subelements) desde DEs contenedores (DE_48, DE_62, DE_123, DE_124, DE_125) en formato TLV (4 chars tag + 3 chars length + value)
- MTI 1644: divide por Function Code (685, 688, 691) — cada FC tiene sus propios PDS tags
- Escribe a `staging/200_IPM_{MTI}_TRA/`

**3. Extract** (`lmbd-mc-extract`) — en validacion end-to-end
Lee los Parquets del transform (capa TRA), alinea el schema contra los layouts de campos en DynamoDB (`mastercard_fields`), renombra columnas tecnicas a nombres de extract estandarizados, rellena columnas faltantes del layout con NA y reordena columnas. Escribe a `staging/300_IPM_{MTI}_EXT/`.
MTIs soportados: 1240, 1442, 1644 (FC 685/688/691), 1740.
Equivalente funcional al extract de Visa.

**4. Clean** (`lmbd-mc-clean`) — en validacion end-to-end
Lee los Parquets del extract (capa EXT), castea y normaliza cada columna segun definiciones de dtype en DynamoDB, aplica conversion de moneda usando tabla de referencia desde S3 (`currency/data.parquet`), aplica orden de columnas deterministico y escribe a la capa CLN usando schema PyArrow. Timeout: 300s, /tmp: 10240 MB.
MTIs soportados: 1240, 1442, 1644 (FC 685/688/691), 1740.
Equivalente funcional al clean de Visa.

**5. Calculate** (`glue-mc-calculate`) — en validacion end-to-end
Calculo de campos derivados para la tarificacion MC sobre PySpark (Glue 4.0). Funciones principales: `calculate_pre2()` (range-join IAR con bucket-prefix), `calculate_ex_rate()` (tipos de cambio desde S3 Hive), `calculate_settlement_report()`, `calculate_final_fields()` (ensamble + jurisdiction_assigned), `build_lookup_691_spark()` + `apply_exclude_flag()`. Lee datos de referencia desde `s3-reference`: country, region, currency, mastercard_brand_product, mastercard_iar. Escribe a `staging/500_IPM_{MTI}_CAL/`.
Equivalente funcional al calculate de Visa.

**6. Interchange** (`glue-mc-interchange`) — en validacion end-to-end
Asigna tarifas IAR a transacciones MTIs 1240 y 1442. Lee CLN + CAL + datos de referencia S3 (`currency/`, `exchange_rate/`, `mc_rules/`). Escribe a `staging/600_IPM_{MTI}_ITX/`.
Nota: a diferencia de Visa, no contrasta contra MTI 1644 (liquidacion) — scope limitado a tarificacion transaccional en esta version.

**7. Store** (`lmbd-mc-store`) — en validacion end-to-end
Consolida CLN + CAL + ITX (si existe) por MTI en un Parquet final y lo escribe a `s3-operational`. Merge horizontal por columnas nuevas (`axis=1`), garantizado por el orden de filas del pipeline.
MTIs con ITX: 1240, 1442. MTIs sin ITX: 1644, 1740.

**8. Archive** (`lmbd-archive-file`)
Mueve el archivo original del landing a `s3-archive`.

---

## Estructura del repositorio

```
interchange-datalake-aws/
├── lambdas/
│   ├── router/                     # Entrada: clasifica y despacha
│   ├── unzip/                      # Descompresion de ZIPs antes del router
│   ├── archive-file/               # Archiva archivos originales post-proceso
│   ├── visa/
│   │   ├── transform/              # EBCDIC → Parquet (BASEII, SMS, VSS)
│   │   ├── extract/                # Mapeo de campos segun DynamoDB
│   │   ├── clean/                  # Validacion y limpieza
│   │   ├── store/                  # Salida final → S3 Operational
│   │   ├── ardef/                  # Motor de reglas ARDEF (fees Visa)
│   │   └── exchange-rates/         # Conversion de moneda
│   └── mastercard/
│       ├── transform/
│       ├── extract/
│       ├── clean/
│       ├── store/
│       ├── iar/                    # Interchange Assessment Rules
│       ├── interpreter/            # Motor de interpretacion de reglas
│       └── exchange-rates/
├── step-functions/
│   ├── visa/asl.json               # Definicion del orquestador Visa (ASL)
│   └── mastercard/asl.json
├── glue/
│   ├── scripts/visa/               # itx-calculate, itx-interchange (Visa)
│   ├── scripts/mastercard/
│   └── *.json                      # Configs de crawlers, databases, tables
├── dynamodb/                        # Schemas y documentacion de tablas
├── s3/                             # Configuraciones de buckets
├── iam/                            # Roles IAM documentados
├── athena/                         # Workgroups y catálogos
├── layers/itx-pandas-pyarrow/      # Layer compartido: pandas + pyarrow
├── infrastructure/
│   ├── deploy.sh                   # Script de despliegue completo
│   └── terraform/                  # IaC Terraform
├── scripts/                        # Utilitarios locales de desarrollo (no se despliegan)
│   ├── sync-lambdas.ps1            # Descarga config + codigo de Lambdas desde AWS al repo
│   └── sync-glue.ps1               # Descarga config + scripts de Glue Jobs desde AWS al repo
└── .env.example                    # Template de variables de entorno
```

Cada Lambda sigue esta estructura interna:
```
lambdas/<marca>/<etapa>/
├── src/handler.py      # Handler principal
├── config.json         # Metadata de la funcion
└── env-vars.json       # Variables de entorno requeridas
```

---

## Servicios AWS y su rol

| Servicio | Rol en el proyecto |
|----------|-------------------|
| **Lambda** (Python 3.11) | Procesamiento por etapas del pipeline |
| **Step Functions** | Orquestacion del flujo completo |
| **S3** (5 buckets) | Data lake por capas |
| **DynamoDB** (4 tablas) | Configuracion y control de estado |
| **Glue** (4 jobs + 8 databases + 8 crawlers) | ETL pesado y catalogo de datos — inventario completo en `glue/GLUE_CATALOG_CREATION.md` |
| **Athena** | Consultas SQL sobre los datos finales |
| **CloudWatch** | Logs (30 dias de retencion) |
| **IAM** | 11 roles con permisos granulares |

---

## S3 — 5 buckets (Data Lake por capas)

Patron de nomenclatura: `itl-0004-itx-{env}-intchg-02-s3-{tipo}`

| Nombre real (env=dev) | Tipo | Proposito |
|-----------------------|------|-----------|
| `itl-0004-itx-dev-intchg-02-s3-landing` | landing | Archivos raw de entrada (trigger del pipeline) |
| `itl-0004-itx-dev-intchg-02-s3-staging` | staging | Parquets intermedios (transform, extract, clean) |
| `itl-0004-itx-dev-intchg-02-s3-operational` | operational | Parquets finales listos para consumo |
| `itl-0004-itx-dev-intchg-02-s3-archive` | archive | Archivos originales post-procesamiento |
| `itl-0004-itx-dev-intchg-02-s3-reference` | reference | Datos de referencia (tablas ARDEF, IAR, tipos de cambio) |

---

## DynamoDB — 4 tablas (diseno configuration-driven)

El pipeline es **configuration-driven**: la logica de clasificacion, mapeo y validacion vive en DynamoDB, no en el codigo.

Patron de nomenclatura: `itl-0004-itx-{env}-dynamo-{tabla}-02`

| Nombre real (env=dev) | PK | Proposito |
|-----------------------|----|-----------|
| `itl-0004-itx-dev-dynamo-file_control-02` | `file_id` | Tracking de archivos procesados (~55 items) |
| `itl-0004-itx-dev-dynamo-file_pattern-02` | `pattern_id` | Patrones regex para detectar tipo de archivo; incluye campos `file_block` e `interpreter_fix` para MC |
| `itl-0004-itx-dev-dynamo-visa_fields-02` | `type_record` (HASH) + `column_name` (RANGE) | Definicion de campos Visa por tipo de archivo (~430 items); GSI `type-record-index` usado por lmbd-vi-clean |
| `itl-0004-itx-dev-dynamo-mastercard_fields-02` | `type_record` | Definicion de DEs y PDS Mastercard por MTI (type_record: DE o PDS) |
| `itl-0004-itx-dev-dynamo-client-02` | `client_id` | Catalogo de clientes; incluye encoding MC por direccion (`file_mc_encoding_in`, `file_mc_encoding_out`) |

---

## Lambda — configuracion de recursos

| Tipo | Memoria | Timeout | Funciones |
|------|---------|---------|-----------|
| Simple | 8192 MB | 240s | router, archive-file, unzip |
| Procesamiento Visa | 10240 MB | 900s | vi-transform, vi-extract, vi-clean, vi-store |
| Procesamiento MC — transform | 10000 MB | 400s | mc-transform |
| Procesamiento MC — interpreter | 10240 MB | 480s | mc-interpreter |
| Procesamiento MC — extract | 10240 MB | 900s | mc-extract |
| Procesamiento MC — clean | 10240 MB | 300s (/tmp 10240 MB) | mc-clean |
| Procesamiento MC — otros | 10240 MB | 900s | mc-store |

**Inventario completo de Lambdas (nombres reales en AWS):**

| Lambda | Nombre real (env=dev) | Confirmado |
|--------|-----------------------|:----------:|
| router | `itl-0004-itx-dev-intchg-02-lmbd-router` | ✓ |
| unzip | `itl-0004-itx-dev-intchg-02-lmbd-unzip` | ✓ |
| archive-file | `itl-0004-itx-dev-intchg-02-lmbd-archive-file` | ✓ |
| vi-transform | `itl-0004-itx-dev-intchg-02-lmbd-vi-transform` | ✓ |
| vi-extract | `itl-0004-itx-dev-intchg-02-lmbd-vi-extract` | ✓ |
| vi-clean | `itl-0004-itx-dev-intchg-02-lmbd-vi-clean` | ✓ |
| vi-store | `itl-0004-itx-dev-intchg-02-lmbd-vi-store` | ✓ |
| vi-ardef | `itl-0004-itx-dev-intchg-02-lmbd-vi-ardef` | ✓ |
| vi-exchange-rates | `itl-0004-itx-dev-intchg-02-lmbd-vi-exchange-rates` | ✓ |
| mc-transform | `itl-0004-itx-dev-intchg-02-lmbd-mc-transform` | ✓ |
| mc-interpreter | `itl-0004-itx-dev-intchg-02-lmbd-mc-interpreter` | ✓ |
| mc-iar | `itl-0004-itx-dev-intchg-02-lmbd-mc-iar` | — |
| mc-extract | `itl-0004-itx-dev-intchg-02-lmbd-mc-extract` | ✓ |
| mc-clean | `itl-0004-itx-dev-intchg-02-lmbd-mc-clean` | ✓ |
| mc-store | `itl-0004-itx-dev-intchg-02-lmbd-mc-store` | ✓ |
| mc-exchange-rates | `itl-0004-itx-dev-intchg-02-lmbd-mc-exchange-rates` | — |

**Chunked processing:** los Lambdas de procesamiento dividen los archivos en chunks para no exceder el timeout:
- `transform`: chunks de 128 MB, flush cada 1,000,000 records
- `extract` / `clean`: chunks de 300,000 filas

**Layer compartido:** `itl-0004-itx-{env}-intchg-02-pandas-pyarrow` (pandas + pyarrow) — todas las Lambdas de procesamiento lo usan.

---

## Glue Jobs

Patron de nomenclatura: `itl-0004-itx-{env}-intchg-02-glue-{marca}-{job}`

| Nombre real (env=dev) | Marca | Workers | Proposito |
|-----------------------|-------|---------|-----------|
| `itl-0004-itx-dev-intchg-02-glue-vi-calculate` | Visa | G.1X × 2 | Calculo de fees por transaccion |
| `itl-0004-itx-dev-intchg-02-glue-vi-interchange` | Visa | G.2X × 4 | Reporte consolidado de interchange |
| `itl-0004-itx-dev-intchg-02-glue-mc-calculate` | MC | G.1X × 2 | Calculo de fees Mastercard |
| `itl-0004-itx-dev-intchg-02-glue-mc-interchange` | MC | G.1X × 2 | Reporte consolidado interchange MC |
| `itl-0004-itx-dev-intchg-02-glue-test-1` | Visa/MC | G.1X × 2 | Reporte de transacciones (`glue-vi-mc-reporting` / `get_transaction.py`) — un cliente por ejecucion. Nombre real pendiente de renombrar (ver Pendientes). |

Glue Version: 4.0

Cada Glue job tiene un `args.json` junto a su script con los `DefaultArguments` usados en AWS (rutas S3, Spark conf, logging). Sirve como documentacion de los argumentos que Step Functions debe pasar al invocar el job.

**Optimizacion vi-calculate (2026-06-02):** `load_visa_ardef` fue migrado de pandas + `toPandas()` a 100% Spark. La deduplicacion y eliminacion de rangos solapados del ARDEF ahora usa `Window.partitionBy` + `row_number()` y `F.lag()`. Eliminado `import pandas as pd` y el parametro `ardef_pd` de todas las firmas. Motivo: el `toPandas()` presionaba la heap del driver y causaba `Py4JError` en archivos grandes.

**Fix vi-calculate — ARDEF en null por parseo de fechas (2026-06-06):** `load_visa_ardef` parseaba `effective_date` (string `yyyyMMdd`, ej. `'20131018'`) con `F.to_date(col)` **sin formato explicito**. Sin formato, `to_date()` espera ISO `yyyy-MM-dd` y devuelve `NULL` para el 100% de las filas — el filtro de fechas vaciaba el ARDEF completo y los 10 campos derivados del cruce (`ardef_country`, `product_id`, `funding_source`, `b2b_program_id`, `fast_funds`, `nnss_indicator`, `product_subtype`, `technology_indicator`, `travel_indicator`, `issuer_country`) salian 100% null en `calculate.parquet`, aunque el job terminaba en SUCCESS con el conteo de filas correcto. Tambien existia un pre-filtro redundante que comparaba `effective_date` (string `yyyyMMdd`) contra `file_date_str` (string `yyyy-MM-dd`) antes de convertir a `DateType` — comparacion lexicografica entre formatos distintos, igualmente incorrecta. Solucion: `F.to_date(F.col("effective_date"), "yyyyMMdd")` (mismo patron ya usado en `mastercard/calculate/calculate.py:826-829`) + eliminacion del pre-filtro de strings (el filtro real, format-agnostic, ya existia despues de convertir ambas fechas a `DateType`). Detalle completo y metodologia de deteccion en `.claude/memory/gotchas.md`.

**Fix vi-interchange — content_hash perdido + token "Space" destruido (2026-06-08):** (1) `evaluate_interchange_fees()` usa `mapInPandas()` que reemplaza el schema completo — `content_hash` no estaba en `OUTPUT_COLS`/`output_schema` y se descartaba silenciosamente. Solucion: agregado como primer elemento de ambos. (2) `_apply_default()` tenia un `value.strip()` extra dentro del loop de parseo de `value_list` que convertia el espacio literal `' '` en `''`, impidiendo que transacciones GR con `acceptance_terminal_indicator=' '` matchearan la regla `intelica_id=39` (GR SECURE CR) y cayeran en el fallback 63 (GR NON-SEC CR). Solucion: eliminado el `.strip()` extra. Validado contra 524 transacciones GR mal clasificadas. Detalle en `.claude/memory/gotchas.md`.

**Fix vi-clean — _parse_dates logica incorrecta para campos de fecha YDDD/MMDD (2026-06-08):** La logica "compute-then-correct" (restar N años si resultado > file_date) era incorrecta para todos los formatos de fecha Visa: `!YDDD` restaba 10 años innecesariamente (causando timeliness ≈ -3653); `!MMDD` comparaba fecha completa en vez de solo el mes (purchase_date retrocedia 1 año cuando no correspondia); conversion_date necesitaba un nuevo formato `!YDDD_MAX` (igual que `!YDDD` pero con cap en file_date). Reescritura completa de `_parse_dates()` con las tres estrategias correctas. DynamoDB actualizado: conversion_date en type_record=draft cambiado de `!YDDD` a `!YDDD_MAX`. Detalle y validacion completa en `.claude/memory/gotchas.md`.

**Fix vi-calculate — calc_timeliness_draft formula de domingos con off-by-one (2026-06-09):** La formula original usaba ventana `[purchase+1, central-1]` con `full_weeks + extra_sunday`, donde `extra_sunday = when(remaining >= days_to_next_sunday, 1)`. Cuando `remaining == days_to_next_sunday`, el domingo cae exactamente fuera del intervalo — la condicion `>=` lo contaba igual, produciendo timeliness 1 menos que el legacy. Solucion: reescritura a formula directa con offset `(8 - start_dow) % 7`: `sundays = max(0, floor((total_days - 1 + 6 - offset) / 7))`. Mas simple, menos columnas temporales, y matematicamente correcto. Detalle y ejemplo concreto en `.claude/memory/gotchas.md`.

**Fix vi-interchange — _apply_default NaN convertia a cadena "nan" en columnas no-SPACE (2026-06-09):** `batch[col].astype(str).str.strip()` convierte NaN a la cadena `"nan"` (len=3, no 0) — el `.mask(len == 0, "BLANK")` no la sustituia, dejando `"nan"` como valor normalizado. Al contrastarlo contra `valid_values` no habia match y la fila caia en el fallback. Solucion: `batch[col].fillna("").astype(str).str.strip()` garantiza `NaN → "" → "BLANK"`. Detalle en `.claude/memory/gotchas.md`.

**Fix vi-interchange — fillna(0.0) en fee_min/fee_cap zeroeaba fees positivos (2026-06-09):** `process_pandas_partitions` aplicaba `.fillna(0.0)` a `interchange_fee_min` e `interchange_fee_cap` antes de yield. Reglas sin cap definido tienen `fee_cap=NaN`; `fillna(0.0)` lo convertia a `0.0`, que Spark recibia como valor (no NULL) — `coalesce(0.0, +inf) = 0.0` → `least(fee_amount, 0.0) = 0` zeroeaba todos los fees positivos de esas reglas. Mismo problema con `fee_min=NaN → 0.0` flooreando fees negativos. Solucion: eliminar `.fillna(0.0)` de ambas columnas (dejar solo `.astype(float)`) — NaN de pandas → NULL en Spark → `coalesce(NULL, ±inf)` → sin restriccion. Detalle en `.claude/memory/gotchas.md`.

**Pendiente vi-interchange — matching incorrecto intelica_id ATM JPY (detectado 2026-06-09):** Transaccion interregional JPY (source_currency=392) asignada a regla 1055 "ATM AF" (fee_variable=0.0015, sin fee_fixed, fee_currency=None) en vez de 1065 "ATM AF JPN" (fee_variable=0.0015, fee_fixed=0.50 USD). La regla 1065 es especifica para ATM en Japon; el nuevo sistema no esta aplicando la condicion que la distingue. Diferencia numerica refleja monedas distintas (0.69 USD legacy vs 30.33 JPY nuevo) — no comparables directamente. Requiere investigar que campo en visa_rules diferencia ambas reglas. Detalle en `.claude/memory/gotchas.md`.

**Fix glue-test-1 (glue-vi-mc-reporting) — load_exchange_rates() leia tabla incompleta con columnas incorrectas, "Column 'to_currency' does not exist" (2026-06-10):** `load_exchange_rates()` en `glue/scripts/reports/get_transaction/get_transaction.py` leia `exchange-rates/brand={brand_path}/exchange_date=YYYY-MM-DD/` (cobertura incompleta) asumiendo columnas `from_currency, to_currency, fx_rate` que no existen ahi (real: `currency_from, currency_to, currency_from_code, currency_to_code, exchange_value`). `_join_exchange_rates()` fallaba con `Column 'to_currency' does not exist` justo despues de cargar 561,711 filas de `baseii_drafts` (confirmando que el fix de NullType de `lmbd-vi-store` funciono) — el job terminaba `SUCCEEDED` pero sin generar reporte (`No data for EBGR in [...], skipping`). Solucion: `load_exchange_rates()` ahora lee `exchange_rate/rate_date=YYYY-MM-DD/` (cubre 2025-12-01..2026-04-30, ambas marcas via columna `brand`='VISA'/'MasterCard', filtro case-insensitive) y renombra `rate_date→exchange_date`, `currency_from→from_currency`, `currency_to→to_currency`, `exchange_value→fx_rate` para no tocar `_join_exchange_rates()`. Nota: hay un nuevo metodo de extraccion de tipo de cambio Visa en desarrollo — revisar `load_exchange_rates()` cuando este disponible. Detalle en `.claude/memory/gotchas.md`.

---

## Convencion de nombres

Patron corporativo base: `itl-0004-itx-{env}-intchg-02-{servicio}-{marca}-{componente}`

Abreviaturas de marca: `vi` = Visa, `mc` = Mastercard

| Recurso | Patron | Ejemplo (env=dev) |
|---------|--------|-------------------|
| Lambda | `itl-0004-itx-{env}-intchg-02-lmbd-{marca}-{etapa}` | `itl-0004-itx-dev-intchg-02-lmbd-vi-transform` |
| Glue Job | `itl-0004-itx-{env}-intchg-02-glue-{marca}-{job}` | `itl-0004-itx-dev-intchg-02-glue-mc-calculate` |
| S3 Bucket | `itl-0004-itx-{env}-intchg-02-s3-{tipo}` | `itl-0004-itx-dev-intchg-02-s3-landing` |
| DynamoDB | `itl-0004-itx-{env}-dynamo-{tabla}-02` | `itl-0004-itx-dev-dynamo-file_control-02` |
| Layer | `itl-0004-itx-{env}-intchg-02-pandas-pyarrow` | `itl-0004-itx-dev-intchg-02-pandas-pyarrow` |
| Step Function Visa | `itl-0004-itx-dev-intchg-02-sfn-vi` | ✓ |
| Step Function MC | `itl-0004-itx-dev-intchg-02-sfn-mc` | ✓ |
| IAM Role Lambda | `itl-0004-itx-{env}-intchg-02-lmbd-{marca}-role` | `itl-0004-itx-dev-intchg-02-lmbd-mc-role` |

---

## Variables de entorno (.env)

Copiar `.env.example` → `.env` y completar:
```
AWS_REGION=eu-south-2
AWS_ACCOUNT_ID=<account-id>
ENVIRONMENT=dev          # dev | staging | prod

S3_BUCKET_LANDING=itl-0004-itx-dev-intchg-02-s3-landing
S3_BUCKET_STAGING=itl-0004-itx-dev-intchg-02-s3-staging
S3_BUCKET_OPERATIONAL=itl-0004-itx-dev-intchg-02-s3-operational
S3_BUCKET_ARCHIVE=itl-0004-itx-dev-intchg-02-s3-archive
S3_BUCKET_REFERENCE=itl-0004-itx-dev-intchg-02-s3-reference

DYNAMODB_TABLE_FILE_CONTROL=itl-0004-itx-dev-dynamo-file_control-02
DYNAMODB_TABLE_FILE_PATTERN=itl-0004-itx-dev-dynamo-file_pattern-02
DYNAMODB_TABLE_VISA_FIELDS=itl-0004-itx-dev-dynamo-visa_fields-02
DYNAMODB_TABLE_CLIENT=itl-0004-itx-dev-dynamo-client-02

STEP_FUNCTION_ARN=arn:aws:states:eu-south-2:<account-id>:stateMachine:itx-visa-orchestrator

CHUNK_SIZE_MB=64
FLUSH_BATCH_SIZE=500000
EXTRACT_CHUNK_SIZE=100000
CLEAN_CHUNK_SIZE=100000
```

**NUNCA commitear `.env` al repositorio.**

---

## Deploy

```bash
git clone <repo>
cd interchange-datalake-aws
cp .env.example .env
# Editar .env con valores del ambiente destino
chmod +x infrastructure/deploy.sh
./infrastructure/deploy.sh
```

El script `deploy.sh` crea en orden: S3 buckets → IAM roles → Lambda layer → DynamoDB tables → Lambdas → S3 event triggers → Step Functions → Glue jobs y crawlers.

Alternativamente, usar Terraform en `infrastructure/terraform/`:
```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

---

## Pendientes conocidos

**Mastercard — en validacion end-to-end:**
- Pipeline MC completo desplegado — validacion en curso con `itx-mastercard-orchestrator`
- Gotchas de mc-transform (timeout multi-MTI, chunking, /tmp, var DDB) pendientes de resolver — ver `.claude/memory/gotchas.md`

**General:**
- `itx-lambda-extract-role`: rol IAM propio para itx-extract (actualmente comparte el del router)
- `itx-glue-crawler-ebgr-role`: rol IAM propio para el crawler Mastercard
- Renombrar crawlers y databases Glue con prefijo `itx-` consistente — verificado 2026-06-06: los 16 objetos planeados en `glue/GLUE_CATALOG_CREATION.md` existen, pero con nombres reales que omiten `intchg` respecto al plan documentado; ademas hay 5 objetos extra (databases/crawlers `poc_*`) con una tercera convencion de nombres distinta. Detalle e inventario completo en la seccion "Estado de verificacion" de ese mismo archivo.
- Mover scripts Glue MC de bucket `itl-0004-itx-dev-poc-02-reference/` al bucket oficial `itl-0004-itx-dev-intchg-02-s3-reference/`
- Configurar retencion de logs en CloudWatch (variable `log_retention_days = 30` en Terraform ya esta lista)
- Testing end-to-end en ambiente empresarial
- Renombrar `glue-test-1` (job real de `glue-vi-mc-reporting` / `get_transaction.py`) a un nombre que siga la convencion (ej. `itl-0004-itx-dev-intchg-02-glue-vi-mc-reporting`); existen ademas `glue-test-2/3/4` sin uso conocido — verificar antes de tocarlos
- `load_exchange_rates()` (reporting) usa `exchange_rate/rate_date=YYYY-MM-DD/` como fuente de tipo de cambio — hay un nuevo metodo de extraccion de tipo de cambio Visa en desarrollo que podria reemplazar/complementar esta fuente; revisar `load_exchange_rates()` cuando este disponible

---

## Tipos de archivo soportados

| Tipo | Marca | Descripcion | Encoding |
|------|-------|-------------|----------|
| BASEII | Visa | Transacciones (TC 05/06/07/25/26/27) | Texto plano, latin-1, ancho fijo |
| SMS | Visa | Transacciones (TC 33) | Texto plano, latin-1, ancho fijo |
| VSS | Visa | Settlement / liquidacion (TC 46, file_type=IN) | Texto plano, latin-1, ancho fijo |
| ARDEF | Visa | Rangos de BINes y reglas de fees | Texto plano, latin-1 |
| IAR | Mastercard | Rangos de BINes y reglas de fees | EBCDIC |
| IN/OUT | Mastercard | Transacciones de interchange | EBCDIC |
| ZIP | Ambas | Contenedor de archivos de cualquier tipo anterior | N/A |

---

## Patrones de desarrollo

- Cada etapa del pipeline es independiente y stateless; el estado se pasa via el payload de Step Functions.
- La logica de negocio (campos, validaciones, fees) vive en DynamoDB, no hardcodeada en el codigo.
- Los archivos Visa son texto plano de ancho fijo (latin-1); los de Mastercard son EBCDIC. En ambos casos `transform` los convierte a Parquet y las etapas posteriores solo trabajan con Parquet.
- Las Lambdas leen y escriben en S3 usando streams para evitar cargar archivos completos en memoria.
- Los Glue jobs leen del catalogo de Glue (crawlers actualizan el schema automaticamente).
- `lmbd-unzip` archiva el ZIP original en `s3-archive` (no en `s3-operational`). Usa `S3_BUCKET_ARCHIVE`. Los archivos extraidos se suben al landing para re-disparar el router.

---

## Scripts de sincronizacion (utilitarios locales)

Scripts PowerShell en `scripts/` para mantener el repo sincronizado con el estado real de AWS. **Solo para uso local del desarrollador — no forman parte del pipeline ni del deploy.**

Prerequisito: `aws sso login --profile itx-dev` y `$env:AWS_PROFILE = "itx-dev"`.

**`sync-lambdas.ps1`** — descarga desde AWS al repo:
- `get-function-configuration` → `config.json`
- Variables de entorno del Lambda → `env-vars.json`
- ZIP del deployment descomprimido → `src/`

```powershell
.\scripts\sync-lambdas.ps1                        # todos
.\scripts\sync-lambdas.ps1 -Group mc              # solo Mastercard
.\scripts\sync-lambdas.ps1 -Group vi              # solo Visa
.\scripts\sync-lambdas.ps1 -Group general         # router, unzip, archive-file
.\scripts\sync-lambdas.ps1 -Lambda mc-interpreter # uno especifico
```

**`sync-glue.ps1`** — descarga desde AWS al repo:
- `get-job` → `config.json`
- `DefaultArguments` → `args.json`
- Script PySpark desde S3 → `glue/scripts/*/`

```powershell
.\scripts\sync-glue.ps1                   # todos
.\scripts\sync-glue.ps1 -Group mc         # solo Mastercard
.\scripts\sync-glue.ps1 -Job vi-calculate # uno especifico
```

---

## Documentacion adicional

Archivos con contexto acumulado del proyecto — decisiones tomadas y problemas encontrados:

- Decisiones de arquitectura: @.claude/memory/decisions.md
- Gotchas y problemas conocidos: @.claude/memory/gotchas.md
- Ejecución manual / debugging paso a paso: @.claude/memory/manual_execution.md
