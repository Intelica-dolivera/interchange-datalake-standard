from datetime import datetime

import pandas as pd

from schema.schema import IP0040T1_OPERATIONAL_COLUMNS


def clean_ip0040t1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Crear columnas faltantes
    for col_name in IP0040T1_OPERATIONAL_COLUMNS:
        if col_name not in df.columns:
            print(
                f"Columna '{col_name}' no encontrada en el DataFrame. "
                "Creando columna con valores nulos."
            )
            df[col_name] = None

    # Limpiar strings
    string_cols = [
        col
        for col in IP0040T1_OPERATIONAL_COLUMNS
        if col in df.columns
    ]

    for col in string_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
        )

    # Numéricos
    df["low_range"] = pd.to_numeric(df["low_range"], errors="coerce")
    df["high_range"] = pd.to_numeric(df["high_range"], errors="coerce")

    # Fecha de procesamiento: YYYYMMDD
    df["app_processing_date"] = pd.to_datetime(
        df["app_processing_date"],
        format="%Y%m%d",
        errors="coerce"
    ).dt.date

    # Campo calculado de vigencia:
    # effective_timestamp + "00" con formato yy + julian day + HHMM
    effective_value = (
        df["effective_timestamp"]
        .astype("string")
        .str.strip()
        .fillna("")
        + "00"
    )

    df["app_date_valid"] = pd.to_datetime(
        effective_value,
        format="%y%j%H%M",
        errors="coerce"
    )

    # Campos operacionales
    df["app_date_end"] = pd.NaT
    df["app_creation_user"] = "pipeline_iar"
    df["app_creation_date"] = datetime.now()

    # Reordenar columnas según layout
    df = df[IP0040T1_OPERATIONAL_COLUMNS]

    return df