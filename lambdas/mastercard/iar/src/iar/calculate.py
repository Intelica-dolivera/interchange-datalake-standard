# file: iar/iar_calculate.py

from pathlib import Path
from datetime import datetime

import pandas as pd

from logs.logger import logger


BUSINESS_KEYS = [
    "app_customer_code",
    "low_range",
    "gcms_product",
]

DEDUP_KEYS = [
    "app_customer_code",
    "low_range",
    "gcms_product",
    "effective_timestamp",
    "app_full_data",
]


def apply_scd2_validity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula app_date_end usando el siguiente app_date_valid
    por llave de negocio.
    """

    df = df.copy()

    df = df.sort_values(
        BUSINESS_KEYS + ["app_date_valid"],
        na_position="last"
    )

    df["_next_app_date_valid"] = (
        df.groupby(BUSINESS_KEYS)["app_date_valid"]
        .shift(-1)
    )

    df["app_date_end"] = df["_next_app_date_valid"] - pd.Timedelta(seconds=1)

    df.loc[df["_next_app_date_valid"].isna(), "app_date_end"] = pd.NaT

    df = df.drop(columns=["_next_app_date_valid"])

    return df


def calculate_ip0040t1_operational(
    df_new: pd.DataFrame, operational_path=None,
) -> pd.DataFrame:
    """
    Capa CALCULATE para IP0040T1.
    """

    df_all = df_new.copy()

    logger.info(f"Registros histórico+nuevo | Registros={len(df_all)}")

    df_all = df_all.drop_duplicates(
        subset=DEDUP_KEYS,
        keep="last"
    )

    logger.info(f"Registros luego dedup | Registros={len(df_all)}")

    df_all["app_creation_user"] = "pipeline_iar"
    df_all["app_creation_date"] = datetime.now()

    df_final = apply_scd2_validity(df_all)

    logger.info(f"SCD2 calculado | Registros={len(df_final)}")

    return df_final