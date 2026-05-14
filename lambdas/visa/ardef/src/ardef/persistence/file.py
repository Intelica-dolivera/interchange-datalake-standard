import io
import os 
from enum import StrEnum, auto

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from ardef.logs.logger import Logger
from ardef.persistence.database import Database

log = Logger(__name__)


class _Layer(StrEnum):
    LANDING = auto()
    STAGING = auto()
    OPERATIONAL = auto()


class FileStorage:
    """
    Capa de I/O sobre S3. Reemplaza el acceso a filesystem local.

    Buckets por capa:
        LANDING     ->  ITX_S3_BUCKET_LANDING       (default: itl-0004-itx-dev-poc-02-landing)
        STAGING     ->  ITX_S3_BUCKET_STAGING       (default: itl-0004-itx-dev-poc-02-staging)
        OPERATIONAL ->  ITX_S3_BUCKET_OPERATIONAL   (default: itl-0004-itx-dev-poc-02-operational)

    Estructura de keys en cada bucket:
        LANDING:        {client_id}/{landing_file_name}
        STAGING:        {client_id}/{brand_id}/{file_type}/{date}/{subdir}/{file_id}.parquet
        OPERATIONAL:    {client_id}/{brand_id}/{file_type}/{date}/{subdir}/{file_id}.parquet
        lu_ardef:       {client_id}/{brand_id}/{file_type}/lu_ardef.parquet
    """

    Layer = _Layer

    def __init__(self) -> None:
        self._s3 = None

    def _get_client(self):
        if self._s3 is None:
            self._s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-south-2"))
        return self._s3
    
    def _get_bucket(self, layer: _Layer) -> str:
        mapping = {
            self.Layer.LANDING: ("ITX_S3_BUCKET_LANDING", "itl-0004-itx-dev-poc-02-landing"),
            self.Layer.STAGING: ("ITX_S3_BUCKET_STAGING", "itl-0004-itx-dev-poc-02-staging"),
            self.Layer.OPERATIONAL: ("ITX_S3_BUCKET_OPERATIONAL", "itl-0004-itx-dev-poc-02-operational"),
        }
        env_var, default = mapping[layer]
        return os.environ.get(env_var, default)
    
    def _get_file_details(self, file_id: str, file_processing_date: str, ) -> dict[str, str]:
        return Database().get_ardef_file_control(
            file_id=file_id,
            file_processing_date=file_processing_date
        )
    
    def _get_s3_key_prefix(
        self, 
        layer: _Layer,
        file_id: str,
        file_processing_date: str, 
        subdir: str = "",
    ) -> str:
        details = self._get_file_details(
            file_id=file_id, 
            file_processing_date=file_processing_date
        )

        if layer == self.Layer.LANDING:
            return f"{details['client_id']}/"
        
        parts = [
            details["client_id"],
            details["brand_id"],
            details["file_type"],
            details["file_processing_date"],
        ]
        if subdir:
            parts.append(subdir)

        return "/".join(parts) + "/"
    
    def _get_s3_key(
            self,
            layer: _Layer,
            file_id: str,
            file_processing_date: str,
            subdir: str = "",
    ) -> str:
        details = self._get_file_details(file_id=file_id, file_processing_date=file_processing_date)
        prefix = self._get_s3_key_prefix(layer, file_id, file_processing_date, subdir)

        if layer == self.Layer.LANDING:
            return prefix + details["landing_file_name"]
        
        return prefix + file_id
    
    def read_plaintext(
            self,
            layer: Layer,
            file_id: str,
            file_processing_date: str,
            subdir: str = "",
            encoding: str = "Latin-1",
    ) -> pd.DataFrame:
        """
        Lee el archivo fuente desde S3 y retorna un Dataframe con columna 'lines'.
        """
        bucket = self._get_bucket(layer)
        key = self._get_s3_key(layer, file_id, file_processing_date, subdir)

        log.logger.debug(f"Leyendo texto: s3//{bucket}/{key}")

        try: 
            response = self._get_client().get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode(encoding)
        except ClientError as exc:
            log.logger.error(
                f"Error S3 [{exc.response['Error']['Code']}] | "
                f"s3://{bucket}/{key}"
            )
            return pd.DataFrame([], columns=["lines"], dtype=str)
        
        lines = [
            line.rstrip("\r\n")
            for line in content.split("\n")
            if line.rstrip("\r\n") != ""
        ]

        return pd.DataFrame(lines, columns=["lines"], dtype=str)
    
    def write_plaintext(self) -> None:
        raise NotImplementedError
    
    def read_parquet(
        self, 
        layer: Layer,
        file_id: str,
        file_processing_date: str,
        subdir: str = "",
    ) -> pd.DataFrame:
        """
        Lee un parquet desde S3 usando un buffer BytesIO en menoria.
        """
        bucket = self._get_bucket(layer)
        key = self._get_s3_key(layer, file_id, file_processing_date, subdir) + ".parquet"

        log.logger.debug(f"Leyendo parquet: s3://{bucket}/{key}")

        response = self._get_client().get_object(Bucket=bucket, Key= key)

        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer)
    
    def read_parquet_by_filepath(self, filepath: str) -> pd.DataFrame:
        """
        Lee un parquet desde el bucket OPERATIONAL usando una S3 key directa.
        Lanza FileNotFoundError si la key no existe (primera ejecución de lu_ardef).
        """
        bucket = self._get_bucket(self.Layer.OPERATIONAL)

        log.logger.debug(f"Leyendo parquet por key: s3//{bucket}/{filepath}")

        try: 
            response = self._get_client().get_object(Bucket=bucket, Key=filepath)
            buffer = io.BytesIO(response["Body"].read())
            return pd.read_parquet(buffer)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"S3 key no encontrada: s3://{bucket}/{filepath}")
            raise

    def write_parquet(
        self,
        data: pd.DataFrame,
        layer: Layer,
        file_id: str,
        file_processing_date: str,
        subdir: str = "",
        index: bool = False,
    ) -> str:
        """
        Serializa un Dataframe como parquet y lo sube a S3. Retorna la S3 URI.
        """
        bucket = self._get_bucket(layer)
        key = self._get_s3_key(layer, file_id, file_processing_date, subdir) + ".parquet"

        buffer = io.BytesIO()
        data.to_parquet(buffer, index=index)
        buffer. seek(0)

        log.logger.debug(f"Escribiendo parquet: s3//{bucket}/{key}")

        self._get_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        return f"s3//{bucket}/{key}"
    
    def write_parquet_by_filepath(
        self,
        data: pd.DataFrame,
        filepath: str,
        index: bool = False,
        *,
        schema: pa.Schema | None = None,
        compression: str = "snappy",
    ) -> None:
        """
        Sube un parquet al bucket OPERATIONAL usando una S3 key directa
        """
        bucket = self._get_bucket(self.Layer.OPERATIONAL)
        buffer = io.BytesIO()

        if schema is None:
            data.to_parquet(buffer, index=index, compression=compression)
        else:
            present = set(data.columns)
            schema_filtered = pa.schema([f for f in schema if f.name in present])
            table = pa.Table.from_pandas(data, schema=schema_filtered, preserve_index=index)
            pq.write_table(table, buffer, compression=compression)

        buffer.seek(0)

        log.logger.debug(f"Escribiendo parquet por key: s3//{bucket}/{filepath}")

        self._get_client().put_object(
            Bucket=bucket, 
            Key=filepath,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

    def get_lu_ardef_filepath(
        self, 
        file_id: str,
        file_processing_date: str,
        filename: str = "lu_ardef.parquet",
    ) -> str:
        """
        Retorna la S3 Key de lu_ardef en el bucket OPERATIONAL.
        Ejemplo: 'BTRLRO/VI/ARDEF/lu_ardef.parquet'
        """
        details = self._get_file_details(file_id, file_processing_date)
        return "/".join([
            details["client_id"],
            details["brand_id"],
            details["file_type"],
            filename,
        ])
    
    def get_list_files_folderpath(
        self,
        layer: Layer,
        file_id: str,
        file_processing_date: str,
        subdir: str = "",
    ) -> list[str]:
        """
        Lista S3 keys de parquets con prefijo file_id bajo el prefijo de la capa.
        """
        bucket = self._get_bucket(layer)
        prefix = self._get_s3_key_prefix(layer, file_id, file_processing_date, subdir)

        try: 
            response = self._get_client().list_objects_v2(Bucket=bucket, Prefix=prefix)
        except ClientError as exc:
            log.logger.error(f"Error listando S3 s3://{bucket}/{prefix}: {exc}")
            return []
        
        return sorted([
            obj["Key"]
            for obj in response.get("Contents", []) 
            if obj["Key"].endswith(".parquet") 
            and obj["Key"].split("/")[-1].startswith(file_id)
        ])