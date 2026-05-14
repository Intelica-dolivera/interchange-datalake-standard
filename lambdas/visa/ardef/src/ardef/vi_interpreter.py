import gc
from datetime import datetime

import pandas as pd
import pyarrow as pa

from ardef.logs.logger import Logger
from ardef.persistence.file import FileStorage

log = Logger(__name__)
fs = FileStorage()

def load_as_text(
    layer: FileStorage.Layer,
    file_id: str,
    file_processing_date: str,
    subdir= "",
    encoding: str = "Latin-1"
) -> pd.DataFrame:
    """
    Funcion auxiliar.
    Lee el archivo ARDEF como texto desde LANDING.
    No escribe parquet.
    """

    return fs.read_plaintext(
        layer=layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=subdir,
        encoding=encoding,
    )
    
def _build_ardef_raw_dataframe(
        records: pd.DataFrame,
        file_id: str,
        file_processing_date: str,
) -> pd.DataFrame:
    """
    Convierte el archivo ARDEF leido como texto en un dataframe.
    """

    if records.empty:
        log.logger.warning(
            f"No records found for file_id={file_id}, "
            f"file_processing_data={file_processing_date}"
        )

        return pd.DataFrame(
            [],
            columns=[
                "file_id",
                "file_processing_date",
                "ardef_version"
                "ardef_header_date",
                "line_no",
                "lines",
            ],
            dtype=str,
        )
    
    lines: list[str] = []
    versions: list[tuple[str, str]] = []

    for record in records["lines"].astype(str):
        record = record.rstrip("\r\n")

        if record[0:2] == "VL" and "C****" not in record:
            lines.append(record)

        if record[0:8] == "AAACTRNG" and record[10:17] == "AEPACRN":
            header_date = record[23:31]
            version_number = record[63:67]
            versions.append((version_number, header_date))

    ultimate_version = None
    ultimate_date = None

    if versions:
        ultimate_version, ultimate_date = max(
            versions,
            key=lambda x: int(x[0]) if str(x[0]).isdigit() else -1,
        )

        date_formated_as = (
            datetime.strptime(str(ultimate_date), "%Y%m%d")
            .date()
            .strftime("%Y-%m-%d")
        )

        destiny_file = (
            datetime.strptime(str(ultimate_date), "%Y%m%d")
            .date()
            .strftime("%y%m%d")
        )

        date_for_name = datetime.strptime(destiny_file, "%y%m%d").strftime("%Y%m%d")

        log.logger.info(
            f"ARDEF header detected"
            f"ultimate_version={ultimate_version}, "
            f"ultimate_date={ultimate_date}, "
            f"date_formated_as={date_formated_as}, "
            f"destiny_file={destiny_file}, "
            f"date_for_name={date_for_name}"
        )

    else:
        log.logger.warning(
            f"No ARDEF header found for file_id={file_id}, "
            f"file_processing_date={file_processing_date}"
        )

    df = pd.DataFrame(
        {
            "file_id": file_id,
            "file_processing_date": file_processing_date,
            "ardef_version": ultimate_version,
            "ardef_header_date": ultimate_date,
            "line_no": range(1, len(lines) +1),
            "lines": lines,
        }
    )

    return df.astype(str)

def interpretate_ardef(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        file_id: str,
        file_processing_date: str,
        origin_subdir: str = "",
        target_subdir: str = "100_ARDEF_RAW",
        encoding: str = "Latin-1",
) -> None:
    
    records = load_as_text(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir,
        encoding=encoding,
    )

    df_raw = _build_ardef_raw_dataframe(
        records=records,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    output_filepath = fs.write_parquet(
        data=df_raw,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False,
    )

    log.logger.info(
        f"ARDEF RAW parquet created successfully: {output_filepath}"
    )

    del records
    del df_raw
