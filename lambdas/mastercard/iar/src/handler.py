import json
from pathlib import Path

from persistence.file import FileStorage
from logs.logger import logger

from iar.raw import extract_raw_layers
from iar.extract import extract_iar_bytes
from iar.transform import transform_iar_table_from_raw, getIPMParameters
from iar.clean import clean_ip0040t1
from iar.calculate import calculate_ip0040t1_operational

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent

TABLES_TO_PROCESS = [
    "IP0040T1",
]

layer = FileStorage.Layer
fs = FileStorage()

def pipeline_iar(
    origin_layer: FileStorage.Layer, 
    target_layer: FileStorage.Layer, 
    client_id: str, 
    file_id: str,
    blocked: bool ,
    ebcdic: bool,
):
    # 1. LANDING PATH
    landing_bytes = fs.read_binary(
        layer=layer.LANDING,
        client_id=client_id,
        file_id=file_id,
    )

    file_config = fs.get_client_details(
         client_id=client_id
    )

    blocked = file_config["file_iar_block"]
    encoding = file_config["file_iar_encoding"]

    landing_bucket, landing_key = fs.get_landing_object(client_id=client_id,file_id=file_id,)
    
    logger.info(f"Inicio pipeline IAR | client_id={client_id} | file_id={file_id}")
    logger.info(f"Archivo origen: s3://{landing_bucket}/{landing_key}")
    logger.info(f"Archivo bloqueado: {blocked}")
    logger.info(f"Archivo encoding: {encoding}")

    # 2. EXTRACT
    
    stream_raw = extract_iar_bytes(
        file_bytes=landing_bytes,
        blocked=blocked,
    )

    df_header, df_catalog, df_records = extract_raw_layers(
        stream=stream_raw,
        source_file=f"s3://{landing_bucket}/{landing_key}",
        encoding=encoding,
    )

    raw_header_path = fs.write_parquet(
        df=df_header,
        layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
        subdir="RAW/header",
        filename=f"{file_id}.header.parquet",

    )
    
    logger.info(f"RAW HEADER generado | Registros={len(df_header)} | Path={raw_header_path}")


    raw_catalog_path = fs.write_parquet(
        df=df_catalog,
        layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
        subdir="RAW/catalog",
        filename=f"{file_id}.catalog.parquet",
    )
    logger.info(f"RAW CATALOG generado | Registros={len(df_catalog)} | Path={raw_catalog_path}")

    raw_records_path = fs.write_parquet(
        df=df_records,
        layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
        subdir="RAW/records",
        filename=f"{file_id}.records.parquet",
    )
    logger.info(f"RAW RECORDS generado | Registros={len(df_records)} | Path={raw_records_path}")

    # 3. TRANSFORM 
    
    params = getIPMParameters()
    
    for table_name in TABLES_TO_PROCESS:
       
        logger.info(f"Procesando tabla: {table_name}")

        df_staging = transform_iar_table_from_raw(
            df_records=df_records,
            df_catalog=df_catalog,
            df_header=df_header,
            table_to_look=table_name,
            params=params,
            client_id=client_id,
            file_id=file_id,
        )
       
        staging_path = fs.write_parquet(
            df=df_staging,
            layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            subdir='TRA',
            filename=f"{file_id}_raw.parquet",
        )
        
        logger.info(
            f"STAGING generado | Tabla={file_id} | Registros={len(df_staging)} | Path={staging_path}"
        )

    # 4. CLEAN 

        df_clean = clean_ip0040t1(df_staging)
        
        clean_path  = fs.write_parquet(
            df=df_clean,
            layer=layer.STAGING,
            client_id=client_id,
            file_id=file_id,
            subdir='CLN',
            filename=f"{file_id}.parquet",
        )

        logger.info(
            f"CLEAN generado | Tabla={file_id} | "
            f"Registros={len(df_clean)} | Path={clean_path}"
        )
        
    # 5.OPERATIONAL 

        operational_ini_path  = fs.write_parquet(
            df=df_clean,
            layer=layer.OPERATIONAL,
            client_id=client_id,
            file_id=file_id,
            subdir='',
            filename=f"{file_id}.parquet",
        )

        logger.info(
            f"OPERATIONAL generado | Tabla={file_id} | "
            f"Registros={len(df_clean)} | Path={operational_ini_path}"
        )
  
    # 6.FOR REFERENCE

        operational_filename = f"data.parquet" #f"IAR_{table_name}.parquet"
        
        try:
            df_existing_operational = fs.read_parquet(
                layer=layer.REFERENCE,
                client_id=client_id,
                file_id=file_id,
                subdir="mastercard_iar",
                filename=operational_filename,
            )

            df_for_calculate = pd.concat(
                [df_existing_operational, df_clean],
                ignore_index=True,
                sort=False,
            )

        except Exception:
            logger.info(
                f"No existe histórico operational en S3. "
                f"Se creará uno nuevo"
            )

            df_for_calculate = df_clean

        df_operational = calculate_ip0040t1_operational(
            df_new=df_for_calculate,
            operational_path=None,
        )

        operational_path = fs.write_parquet(
            df=df_operational,
            layer=layer.REFERENCE,
            client_id=client_id,
            file_id=file_id,
            subdir="mastercard_iar",
            filename=operational_filename,
        )

        logger.info(
            f"OPERATIONAL generado | Tabla={table_name} | "
            f"Registros={len(df_operational)} | Path={operational_path}"
        )


def lambda_handler(event, context):
    client_id = event.get("client_id")
    file_id = event.get("file_id")

    if not client_id or not file_id:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "Falta client_id o file_id"
            })
        }

    pipeline_iar(
        origin_layer=layer.LANDING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
        blocked=True,
        ebcdic=False,
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "pipeline ejecutado correctamente",
            "client_id": client_id,
            "file_id": file_id
        })
    }