from datetime import datetime

import pandas as pd 
import pyarrow as pa 

from ardef.logs.logger import Logger
from ardef.persistence.file import FileStorage
from ardef.schema.ardef_schema import ARDEF_SCHEMA

log = Logger(__name__)
fs = FileStorage()

def _apply_schema(
    line: str,
    schema: dict[str, dict[str, int]],
) -> dict[str, str]:
    """
    Extrae cada campo de la línea usando solo start/end del schema.
    En campo data_type se ignora aqui
    """
    
    return {
        col: line[spec["start"]: spec["end"]]
        for col, spec in schema.items()
    }

def _build_ardef_transform_dataframe(
    raw: pd.DataFrame,
    file_id: str,
    file_processing_date: str,
    schema: dict[str, dict[str, int]],
) -> pd.DataFrame:
    """
    A partir del parquet RAW aplica el schema de posiciones fijas y devuelve un Dataframe
    con una columna por campo. Todo el contenido por defecto es str.

    El campo 'lines' se arrastra como columna meta para servir de llave natural 
    de deduplicación en etapas posteriores (vi_calculate).
    """
    if raw.empty:
        log.logger.warning(
            f"RAW parquet vacio para file_id={file_id}, "
            f"file_processing_date={file_processing_date}"
        )
        meta_cols = [
            "file_id", "file_processing_date", "ardefe_version", 
            "ardef_header_date", "line_no", "lines"
        ]
        return pd.DataFrame([], columns=meta_cols + list(schema.keys()), dtype=str)
    
    log.logger.info(
        f"Aplicando schema ARDEF sobre {len(raw)} lineas | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    # Parsear cada linea aplicando los rangos del schema
    parsed = raw["lines"].apply(lambda line: _apply_schema(line=line, schema=schema))
    parsed_df = pd.DataFrame(parsed.tolist(), index=raw.index)

    # Metadatos que se arrastran del parquet RAW
    # ' lines' se mantiene como llave natural de deduplicación
    meta = raw[[
        "file_id", "file_processing_date", "ardef_version", 
        "ardef_header_date", "line_no", "lines",
    ]].reset_index(drop=True)
    parsed_df = parsed_df.reset_index(drop=True)

    df = pd.concat([meta, parsed_df], axis=1)

    return df.astype(str)

def transform_ardef(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    file_id: str,
    file_processing_date: str,
    origin_subdir: str = "100_ARDEF_RAW",
    target_subdir: str = "200_ARDEF_TRA",
    schema: dict[str, dict[str, int]] | None = None
) -> None:
    """
    Lee el parquet RAW de ARDEF, aplica la plantilla de posiciones fijas y escribe el resultado 
    en un nuevo parquet en STAGING.

    Pasos: 
        1. Leer STAGING / {brand_id} / {file_type} / {date} / 100_ARDEF_RAW / {file_id}.parquet
        2. Parsear cada línea según ARDEF_SCHEMA (o el schema que se pasa).
           El campo 'lines' se conserva como columna meta para deduplicación posterior.
        3. Escribir STAGING / {brand_id} / {file_type} / {date} / 200_ARDEF_TRA / {file_id}.parquet
    """

    if schema is None:
        schema = ARDEF_SCHEMA

    log.logger.info(
        f"Inicio transform_ardef | " 
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    #1. Leer Raw
    raw = fs.read_parquet(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir
    )

    #2. Parsear
    df_transform = _build_ardef_transform_dataframe(
        raw=raw,
        file_id=file_id,
        file_processing_date=file_processing_date,
        schema=schema,
    )

    #3. Escribir
    output_filepath = fs.write_parquet(
        data=df_transform,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False
    )

    log.logger.info(
        f"ARDEF TRANSFORM parquet creado exitosamente: {output_filepath}"
    )