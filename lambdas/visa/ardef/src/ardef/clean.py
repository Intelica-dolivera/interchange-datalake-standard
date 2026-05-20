from datetime import datetime
from typing import Optional

import pandas as pd 
import numpy as np

from ardef.logs.logger import Logger 
from ardef.persistence.file import FileStorage
from ardef.schema.ardef_schema import ARDEF_SCHEMA, METADATA_SCHEMA

log = Logger(__name__)
fs = FileStorage()

_DATE_FORMATS = ["%Y-%m-%d", "%Y%m%d", "%d%m%Y"]

def _try_parse_date(value: str) -> Optional[pd.Timestamp]:
    """
    Intentar parser una cadena de fecha con múltiples formatos.
    Devuelve NaT si ninguno aplica
    """
    for fmt in _DATE_FORMATS:
        try:
            return pd.Timestamp(datetime.strptime(value.strip(), fmt).date())
        except (ValueError, AttributeError):
            continue
    return pd.NaT

def _cast_series(series: pd.Series, data_type: str) -> pd.Series:
    """
    Castea una Series de pandas según el data_type indicado en el schema.
    """

    match data_type:
        case "text":
            return series.astype(str).str.strip()
        
        case "integer":
            coerced = pd.to_numeric(series.str.strip(), errors="coerce")
            return coerced.astype("Int64")
        
        case "decimal":
            coerced = pd.to_numeric(series.str.strip(), errors="coerce")
            return coerced.astype("Float64")
        
        case "date":
            return series.str.strip().apply(_try_parse_date)
        
        case _:
            log.logger.warning(
                f"data_type '{data_type}' no reconocido - se aplica cast a texto"
            )
            return series.astype(str).str.strip()
        
def _build_ardef_clean_dataframe(
        transformed: pd.DataFrame,
        file_id: str,
        file_processing_date: str,
        ardef_schema: dict[str, dict],
        metadata_schema: dict[str, dict],
) -> pd.DataFrame:
    """
    Aplicar ltrim/rtrim a todos los campos texto
    Castear los campos de metadatos según METADATA_SCHEMA.
    Castear los campos ARDEF según ARDEF_SCHEMA.
    """
    if transformed.empty:
        log.logger.warning(
            f"TRANSFORM parquet vacio para file_id {file_id}, "
            f"file_processing_date={file_processing_date}"
        )
        all_cols = list(metadata_schema.keys()) + list(ardef_schema.keys())
        return pd.DataFrame([], columns=all_cols)
    
    log.logger.info(
        f"Iniciando limpieza y casteo | {len(transformed)} lineas | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )
    
    df = transformed.copy()

    for col, spec in metadata_schema.items():
        if col not in df.columns:
            log.logger.warning(f"Columna de metadato '{col}' no encontrada en el parquet.")
            continue

        df[col] = _cast_series(df[col], spec["data_type"])

    for col, spec in ardef_schema.items():
        if col not in df.columns:
            log.logger.warning(f"Campo ARDEF '{col}' no encontrado en el parquet")
            continue
        df[col] = _cast_series(df[col], spec["data_type"])

    log.logger.info(
        f"Limpieza y casteo completados | file_id = {file_id}, "
        f"file_processing_date={file_processing_date}"
    )

    return df

def clean_ardef(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        file_id: str,
        file_processing_date: str,
        origin_subdir: str = "200_ARDEF_TRA",
        target_subdir: str = "300_ARDEF_CLN",
        ardef_schema: dict[str, dict] | None = None,
        metadata_schema: dict[str, dict] | None = None,
) -> None:
    """
    Lee el parquet TRANSFORM, aplica limpieza y casteo y escribe en STAGING/300_ARDEF_CLN

    Pasos:
        1. Leer STAGING / {brand_id} / {file_type} / {date} / 200_ARDEF_TRA / {file_id}.parquet
        2. Limpiar (strip) y castear según METADATA_SCHEMA + ARDEF_SCHEMA.
        3. Escribir STAGING / {brand_id} / {file_type} / {date} / 300_ARDEF_CLN / {file_id}.parquet
    """
    if ardef_schema is None:
        ardef_schema = ARDEF_SCHEMA

    if metadata_schema is None:
        metadata_schema = METADATA_SCHEMA

    log.logger.info(
        f"Inicio clean_ardef | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    # 1. Leer TRANSFORM
    transformed = fs.read_parquet(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir,
    )

    # 2. Limpiar y castear
    df_clean = _build_ardef_clean_dataframe(
        transformed=transformed,
        file_id=file_id,
        file_processing_date=file_processing_date,
        ardef_schema=ardef_schema,
        metadata_schema=metadata_schema,
    )

    # 3. Escribir
    output_filepath = fs.write_parquet(
        data = df_clean,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False,
    )

    log.logger.info(
        f"ARDEF CLEAN parquet creado exitosamente: {output_filepath}"
    )
