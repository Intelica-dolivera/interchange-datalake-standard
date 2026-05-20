import os
import boto3
import pandas as pd


class Database:
    def __init__(self):
        self.dynamodb = boto3.resource("dynamodb")
        self.client_table_name = os.environ["DDB_CLIENT_TABLE"]
        self.file_control_table_name = os.environ["DDB_FILE_CONTROL_TABLE"]

    def read_records(
        self,
        table_name: str,
        fields: list[str],
        where: dict = {},
    ) -> pd.DataFrame:

        if table_name == "client":
            table = self.dynamodb.Table(self.client_table_name)

            response = table.get_item(
                Key={
                    "client_id": where["client_id"]
                }
            )

            item = response.get("Item")

        elif table_name in ("file_control"):
            table = self.dynamodb.Table(self.file_control_table_name)

            response = table.get_item(
                Key={
                    "file_id": where["file_id"]
                }
            )

            item = response.get("Item")

        else:
            return pd.DataFrame(columns=fields)

        if not item:
            return pd.DataFrame(columns=fields)

        return pd.DataFrame(
            [[item.get(field) for field in fields]],
            columns=fields
        )