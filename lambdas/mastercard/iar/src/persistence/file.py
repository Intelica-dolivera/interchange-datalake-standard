from enum import StrEnum, auto
import os
import io
from typing import Any

import boto3
import pandas as pd

from persistence.database import Database


class _Layer(StrEnum):
    LANDING = auto()
    RAW = auto()
    STAGING = auto()
    OPERATIONAL = auto()
    REFERENCE = auto()

class FileStorage:
    Layer = _Layer

    def __init__(self) -> None:
        self.s3 = boto3.client("s3")

    def _get_file_details(self, client_id: str, file_id: str):
        db = Database()

        df = db.read_records(
            table_name="file_control",
            fields=[
                "file_processing_date",
                "landing_file_name",
            ],
            where={
                "client_id": client_id,
                "file_id": file_id,
            },
        )

        if df.empty:
            raise ValueError(f"No se encontró file_id={file_id}")

        return df.iloc[0]

    def _get_bucket_by_layer(self, layer: _Layer) -> str:
        if layer == self.Layer.LANDING:
            return os.environ["S3_LANDING_BUCKET"]

        if layer == self.Layer.STAGING:
            return os.environ["S3_STAGING_BUCKET"]

        if layer == self.Layer.OPERATIONAL:
            return os.environ["S3_OPERATIONAL_BUCKET"]

        if layer == self.Layer.REFERENCE:
            return os.environ["S3_REFERENCE_BUCKET"]

        raise ValueError(f"No existe bucket configurado para layer={layer}")

    def _build_key(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        filename: str | None = None,
    ) -> str:
        file_details = self._get_file_details(client_id, file_id)

        processing_date = str(file_details["file_processing_date"])
        landing_file_name = file_details["landing_file_name"]

        if layer == self.Layer.LANDING:
            return f"{client_id}/{landing_file_name}"

        if layer == self.Layer.REFERENCE:
            return f"{subdir}/{filename}"

        if layer == self.Layer.OPERATIONAL:
            return f"{client_id}/MC/IAR/"f"date={processing_date}/{filename}"

        if not filename:
            raise ValueError("filename es obligatorio para STAGING/OPERATIONAL")

        return f"{client_id}/MC/IAR/"f"date={processing_date}/"f"process={subdir}/{filename}"

    def get_client_details(
        self,
        client_id: str,
        fields: list[str] | None = None,
    ) -> dict[str, Any]:

        if fields is None:
            fields = [
                "file_iar_block",
                "file_iar_encoding",
            ]

        # TEMPORAL si no tienes permiso a tabla client:
        # return {
        #     "file_iar_block": True,
        #     "file_iar_encoding": "Latin-1",
        # }

       
        db = Database()
        row = db.read_records(
            table_name="client",
            fields=fields,
            where={"client_id": client_id},
        ).iloc[0]
        
        return {field: row.loc[field] for field in fields}

    def get_landing_object(
        self,
        client_id: str,
        file_id: str,
    ) -> tuple[str, str]:

        bucket = self._get_bucket_by_layer(self.Layer.LANDING)

        key = self._build_key(
            layer=self.Layer.LANDING,
            client_id=client_id,
            file_id=file_id,
        )

        return bucket, key

    def read_binary(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
    ) -> bytes:

        bucket = self._get_bucket_by_layer(layer)

        key = self._build_key(
            layer=layer,
            client_id=client_id,
            file_id=file_id,
        )

        response = self.s3.get_object(
            Bucket=bucket,
            Key=key,
        )

        return response["Body"].read()

    def write_parquet(
        self,
        df: pd.DataFrame,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        filename: str = "data.parquet",
    ) -> str:

        bucket = self._get_bucket_by_layer(layer)

        key = self._build_key(
            layer=layer,
            client_id=client_id,
            file_id=file_id,
            subdir=subdir,
            filename=filename,
        )

        buffer = io.BytesIO()

        df.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
        )

        buffer.seek(0)

        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
        )

        return f"s3://{bucket}/{key}"

    def read_parquet(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        filename: str = "data.parquet",
    ) -> pd.DataFrame:

        bucket = self._get_bucket_by_layer(layer)

        key = self._build_key(
            layer=layer,
            client_id=client_id,
            file_id=file_id,
            subdir=subdir,
            filename=filename,
        )

        response = self.s3.get_object(
            Bucket=bucket,
            Key=key,
        )

        return pd.read_parquet(
            io.BytesIO(response["Body"].read())
        )