import gc 
import json 
import logging

from ardef import vi_interpreter, vi_transform, vi_clean, vi_calculate, vi_operational
from ardef.persistence.file import FileStorage

# Logger estándar 
# Lambda captura automáticamente todo lo que va a stout/stderr
# y lo envía a CloudWatch logs sin configuración adicional.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

layer = FileStorage.Layer


def _pipeline_ardef(file_id: str, file_processing_date: str) -> None:
    """
    Orquesta las 5 etapas del pipeline ARDEF en orden.
    """
    vi_interpreter.interpretate_ardef(
        origin_layer=layer.LANDING,
        target_layer=layer.STAGING,
        file_id=file_id,
        file_processing_date=file_processing_date
    )
    
    vi_transform.transform_ardef(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    vi_clean.clean_ardef(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    vi_calculate.calculate_ardef(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    vi_operational.load_operational_ardef(
        origin_layer=layer.STAGING,
        target_layer=layer.OPERATIONAL,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    gc.collect()


def _extract_event_params(event:dict) -> tuple[str, str]:
    """
    Extrae y valida file_id y file_processing_date del evento.

    El router invoca este Lambda con InvocationType='Event' (asíncrono)
    pasando el siguiente payload en variables_input:

        {
            "client_id":      "BTRLRO",
            "file_id":        "1606e40fdc88e10521c619ef69666528",
            "filename":       "20260424repository.ardef.txt",
            "s3_key_landing": "BTRLRO/20260424repository.ardef.txt",
            "bucket_landing": "itl-0004-itx-dev-poc-02-landing",
            "brand":          "VISA",
            "brand_id":       "VI",
            "file_type":      "ARDEF",
            "file_date":      "2026-04-24",       <-- clave que usa el router
            "content_hash":   "ABC123...",
        }


    """
    file_id: str = event.get("file_id", "").strip()

    # El router usa 'file_date'; en ardef usa usa 'file_processing_date'
    file_processing_date: str = event.get("file_date", "").strip()

    missing = []
    if not file_id:
        missing.append("file_id")
    if not file_processing_date:
        missing.append("file_date")

    if missing:
        raise ValueError(
            f"Payload inválido - faltan campos obligatorios: {', '.join(missing)}. "
            f"Event recibido: {json.dumps(event)}"
        )
    
    return file_id, file_processing_date

def lambda_handler(event, context):
    """
    Punto de entrada del Lambda.

    Invocado asincrónamente por el router (VISA_ARDEF_FUNCTION_NAME)
    cuando detecta un archivo con direction=ARDEF

    En este punto el router ha:
        - Registrado el archivo en DynamoDB (file_control) con status=PROCESSING
        - Extraído la fecha del dehader ARDEF vía extrar_fecha_ardef()
        - Calculado el content_hash y verificado que no es duplicado

    Args:
        event: dict con el payload del router (_extract_evetn_params)
        context: objeto con info de la ejecución Lambda (tiempo restante, etc.)

    Returns:
        dict con statusCode 200 si el pipeline completa sin errores, 
        400 si el payload está mal formado, 
        500 si ocurre un error durante el pipeline.
    """
    logger.info("=== Inicio pipeline ARDEF ===")
    logger.info(f"Event recibido: {json.dumps(event)}")

    # Extrar y validar parámetros del evento
    try:
        file_id, file_processing_date = _extract_event_params(event)
    except ValueError as exc:
        logger.error(f"Payload inválido: {exc}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(exc)})
        }
    
    # log de contexto
    logger.info(
        f"Iniciando pipeline | "
        f"file={file_id} | "
        f"file_processing_date={file_processing_date} | "
        f"client_id={event.get('client_id', 'N/A')} | "
        f"filename={event.get('filename', 'N/A')} | "
        f"brand={event.get('brand', 'N/A')} | "
        f"s3_key_landing={event.get('s3_key_landing', 'N/A')}"
    )

    try:
        _pipeline_ardef(
            file_id=file_id,
            file_processing_date=file_processing_date,
        )

        logger.info("=== Pipeline ARDEF completado exitosamente ===")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Pipeline ARDEF completado exitosamente",
                "file_id": file_id,
                "file_processing_date": file_processing_date,
                "client_id": event.get("client_id"),
                "filename": event.get("filename"),
            }),
        }
    
    except Exception as exc:
        logger.error(f"Error en pipeline en ARDEF: {exc}", exc_info=True)

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": f"Error: {str(exc)}",
                "file_id": file_id,
                "file_processing_date": file_processing_date,
                "client_id": event.get("client_id"),
                "filename": event.get("filename"),
            }),
        }