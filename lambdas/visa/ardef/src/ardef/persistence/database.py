import os
from datetime import date

import boto3 
from botocore.exceptions import ClientError

from ardef.logs.logger import Logger

log = Logger(__name__)

class Database:
    """
    Acceso a la table file_control en DynamoDB.
    """

    DEFAULT_TABLE = 'itl-0004-itx-dev-dynamo-file_control-02'

    def __init__(self) -> None:
        self.table_name = os.environ.get("ITX_TABLE_FILE_CONTROL", self.DEFAULT_TABLE)
        self.region = os.environ.get("AWS_REGION", "eu-south-2")
        self._dynamodb = None

    def _get_resource(self):
        if self._dynamodb is None:
            self._dynamodb = boto3.resource("dynamodb", region_name=self.region)
        return self._dynamodb
    
    def get_ardef_file_control(
        self,
        file_id: str,
        file_processing_date: str,
        fields: list[str] | None = None,
    ) -> dict[str, str]:
        """
        Lee el registro de un archivo desde DynamoDB por file_id.
        Valida que file_processing_date conicida con el registro encontrado.
        
        Returns:
            dict[str, str] con los campos solicitados.
        
        Raises:
            ValueError: si el registro no existe o la fecha no coincide.
            ClientError: si hay error de comunicación con DynamoDB
        """
        if fields is None:
            fields = [
                "file_id",
                "client_id",
                "brand_id",
                "file_type",
                "file_processing_date",
                "landing_file_name",
            ]

        log.logger.debug(f"DynamoDB get_item | table={self.table_name} | file_id={file_id}")

        try:
            table = self._get_resource().Table(self.table_name)
            response = table.get_item(Key={"file_id": file_id})
        except ClientError as exc:
            log.logger.error(
                f"Error DynamoDB [{exc.response['Error']['Code']}] | "
                f"tabla={self.table_name} | file_id={file_id}"
            )
            raise

        if "Item" not in response:
            raise ValueError(
                f"No existe registro en file_control para "
                f"file_id={file_id}, file_processing_date={file_processing_date}"
            )
        
        item = response["Item"]

        store_date = _normalize_date(item.get("file_processing_date", ""))
        expected_date = str(file_processing_date).strip()

        if store_date != expected_date:
            raise ValueError(
                f"file_processing_date no coincide | "
                f"esperado={expected_date} | encontrado={store_date} | "
                f"file_id={file_id}"
            )
        
        log.logger.debug(
            f"Registro encontrado | file_id={file_id} | "
            f"client_id={item.get('client_id')} | brand_id={item.get('brand_id')}"
        )

        return {
            field: (
                _normalize_date(item[field])
                if field == "file_processing_date" and field in item
                else str(item.get(field, ""))
            )
            for field in fields
        }
    

def _normalize_date(value) -> str:
    """
    Convierte cualquier representación de fecha a string YYYY-MM-DD.
    """
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value).strip()

