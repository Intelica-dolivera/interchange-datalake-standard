import pandas as pd 

from ardef.logs.logger import Logger
from ardef.persistence.file import FileStorage

log = Logger(__name__)
fs = FileStorage()

def _build_ardef_operational_dataframe(
        clean: pd.DataFrame,
        file_id: str, 
        file_processing_date: str,
) -> pd.DataFrame:
    """
    Recibe el dataframe CLEAN (300_ARDEF_CLN) y lo devuelve para presistir 
    en la capa OPERATIONAL.

    No se aplica mas reglas de transformación adicionales.
    """

    if clean.empty:
        log.logger.warning(
            f"CLEAN parquet vacío para file_id={file_id}, "
            f"file_processing_date={file_processing_date}"
        )
        return clean.copy()
    
    log.logger.info(
        f"Preparando carga operacional | {len(clean)} registros | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    return clean.copy()

def load_operational_ardef(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        file_id: str,
        file_processing_date: str,
        origin_subdir: str = "400_ARDEF_CAL",
        target_subdir: str = "500_ARDEF_OPE",
) -> None:
    """
    Lee el parquet CLEAN de ARDEF y lo persiste en la capa OPERATIONAL.

    1. Leer STAGING 
    2. Preparar Dataframe Operacional
    3. Escribir en OPERATIONAL
    """

    log.logger.info(
        f"Inicio load_operational_ardef | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    #1. Leer CLEAN
    clean = fs.read_parquet(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir
    )

    #2. Preparar
    df_operational = _build_ardef_operational_dataframe(
        clean=clean,
        file_id=file_id,
        file_processing_date=file_processing_date
    )

    #3. Escribir
    output_filepath = fs.write_parquet(
        data=df_operational,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False,
    )

    log.logger.info(
        f"ARDEF OPERATIONAL parquet creado exitosamente: {output_filepath}"
    )