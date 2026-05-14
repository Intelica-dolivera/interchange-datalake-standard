from datetime import datetime, timedelta
from typing import Optional

import pandas as pd 

from ardef.logs.logger import Logger
from ardef.persistence.file import FileStorage

log = Logger(__name__)
fs = FileStorage()

_MATCH_KEYS = ["table_key", "low_key_for_range", "delete_indicator"]
_EFFECTIVE_DATE_COL = "effective_date"
_DATE_VALID_COL = "date_valid"
_LINES_COL = "lines"
_LU_ARDEF_FILENAME = "lu_ardef.parquet"
_DATE_FORMATS = ["%Y%m%d", "%Y-%m-%d", "%d%m%Y"]


def _parse_effective_date_series(series: pd.Series) -> pd.Series:
    """
    Convierte una Serie de cadenas a pd.Timestamp probando múltiples formatos.
    Devuelve NaT cuando no es posible parsear.
    """

    def _try(val: str) -> Optional[pd.Timestamp]:
        if pd.isna(val) or str(val).strip() in ("", "nan", "NaT", "None"):
            return pd.NaT
        for fmt in _DATE_FORMATS:
            try:
                return pd.Timestamp(datetime.strptime(str(val).strip(), fmt).date())
            except ValueError:
                continue
        return pd.NaT
    
    return series.apply(_try)


def _load_lu_ardef(filepath: str) -> pd.DataFrame:
    """
    Carga lu_ardef desde S3. 'filepath' es una S3 key, no una ruta local.
    Retorna Dataframe vacío si la key no exists (primera ejecución).
    """
    try:
        df = fs.read_parquet_by_filepath(filepath)

        if _DATE_VALID_COL not in df.columns:
            df[_DATE_VALID_COL] = pd.NaT

        log.logger.info(f"lu_ardef.parquet cargado: {len(df)} filas | key={filepath}")
        return df
    
    except FileNotFoundError:
        log.logger.info(
            f"lu_ardef.parquet no encontrado en key={filepath}. "
            f"Se asume primera ejecución."
        )
        return pd.DataFrame()
    
    except Exception as exc:
        log.logger.error(f"Error al leer lu_ardef.parquet (key={filepath}): {exc}")
        return pd.DataFrame()

def _deduplicate_incoming(
    df: pd.DataFrame,
    lu_ardef: pd.DataFrame,
    file_id: str,
    file_processing_date: str, 
) -> pd.DataFrame:
    """
    Elimina de las filas entrantes aquellas cuyo campo 'lines' ya existe en lu_ardef.

    El campo 'lines' es la línea de texto original del archivo fuente, y actúa como 
    llave natural única de cada registro: si 'lines' ya esta en lu_ardef, el registro 
    es idéntico al que procesó en una ejecución anterior -> se descarta.
    """
    if lu_ardef.empty:
        return df
    
    if _LINES_COL not in lu_ardef.columns or _LINES_COL not in df.columns:
        log.logger.warning(
            f"Columna {_LINES_COL} ausente; se omite deduplicación | "
            f"file_id={file_id}, file_processing_date={file_processing_date}"
        )
        return df
    
    existing_lines: set = set(lu_ardef[_LINES_COL].dropna())
    duplicate_mask = df[_LINES_COL].isin(existing_lines)
    n_duplicates = int(duplicate_mask.sum())

    if n_duplicates > 0:
        log.logger.info(
            f"{n_duplicates} registro(s) duplicado(s) ignorados "
            f"(lines ya presente en lu_ardef) | "
            f"file_id={file_id}, file_processing_date={file_processing_date}"
        )

    return df[~duplicate_mask].reset_index(drop=True)
    

def _build_calculate_dataframe(
        clean: pd.DataFrame,
        lu_ardef: pd.DataFrame,
        file_id: str,
        file_processing_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcula date_valid para los registros nuevos y actualiza lu_ardef.

    Flujo:
    Paso 0: Deduplicación por 'lines':
        Registros cuyo campo 'lines' ya existe en lu_ardef son idénticos a los ya 
        procesados -> se descartan antes de cualquier otra lógica.

    Paso 1: Actualizar registros ANTERIORES en lu_ardef
        Para cada fila nueva con effective_date E, los registros de lu con las mismas 
        claves y _eff_ts < E reciben: 
            date_valid = min(date_valid_actual, E - 1d)

    Paso 2: date_valid para filas nuevas out-of-order:
        Si en lu existe un registro con las mismas claves y _eff_ts > E:
            date_valid_nueva = min(eff_sucesor_en_lu) - 1d
      
    Ejemplo:
    1er Momento -> archivo 5-may cargado:
        lu: [eff=5may, dv=NaT]
    
    2do momento -> archivo 20-mayo cargado:
        lu: [eff=5may, dv=19may] [eff=20may, dv=NaT]
    
    3er momento -> archivo 15-may cargado (retrasado):
        lu: [eff=5may, dv=14may] [eff=15may, dv=19] [eff=20may, dv=NaT]
    Re-ejecución del archivo 20-may:
        -> todos los registros de ese archivo tienen 'lines' en lu_ardef' -> descartados.

    Retorna:
    * df_calculate: nuevas filas con date_valid calculado (400_ARDEF_CAL)
    * updated_lu_ardef: lu_ardef completo actualizado (OPERATIONAL)
    """

    # nuevas filas con date_valid inicializado a NaT
    df = clean.copy()
    df[_DATE_VALID_COL] = pd.NaT

    # DataFrame CLEAN Vacio
    if clean.empty:
        log.logger.warning(
            f"CLEAN parquet vacío para file_id={file_id}, "
            f"file_processing_date={file_processing_date}"
        )
        updated_lu_ardef = lu_ardef.copy() if not lu_ardef.empty else df.copy()
        return df, updated_lu_ardef
    
    # Paso 0: Deduplicar por 'lines'
    df = _deduplicate_incoming(
        df=df,
        lu_ardef=lu_ardef,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    # Si todas las filas eran -> lu_ardef sin cambios, CAL vacio
    if df.empty:
        log.logger.info(
            f"Todos los registros del archivo son duplicados de lu_ardef."
            f"Sin cambios en la maestra | "
            f"file_id={file_id}, file_processing_date={file_processing_date}"
        )
        empty_cal = pd.DataFrame(columns=clean.columns.tolist() + [_DATE_VALID_COL])
        return empty_cal, lu_ardef.copy() if not lu_ardef.empty else empty_cal
    
    # Parsear effective_date de las nuevas filas (columna temporal)
    df["_eff_ts"] = _parse_effective_date_series(df[_EFFECTIVE_DATE_COL].astype(str))

    # Primera ejecucion: lu_ardef_vacio
    if lu_ardef.empty:
        log.logger.info(
            "lu_ardef vacío (primera ejecución). "
            "Todas las filas nuevas quedan con date_valid = NaT"
        )
        df_out = df.drop(columns=["_eff_ts"])
        return df_out, df.copy()
    
    # Ejecucion con lu_ardef existentes
    log.logger.info(
        f"Aplicando lógica de date_valid contra lu_ardef ({len(lu_ardef)} filas) "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    lu = lu_ardef.copy()
    lu["_eff_ts"] = _parse_effective_date_series(lu[_EFFECTIVE_DATE_COL].astype(str))
    lu = lu.reset_index(drop=True)
    lu["_lu_idx"] = lu.index

    # Tablas de claves para los JOINS
    df_keys = df[_MATCH_KEYS + ["_eff_ts"]].rename(columns={"_eff_ts": "_new_eff_ts"})
    lu_keys = lu[_MATCH_KEYS + ["_lu_idx", "_eff_ts", _DATE_VALID_COL]]

    # Paso 1 - Actualizar registros anteriores en lu_ardef
    merged_prev = lu_keys.merge(df_keys, on=_MATCH_KEYS, how="left")
    merged_prev = merged_prev[merged_prev["_new_eff_ts"] > merged_prev["_eff_ts"]]

    lu_updated_count = 0
    if not merged_prev.empty:
        # Para cada lu_idx: minimo de todos los new_eff_ts sucesores
        min_new_eff = (
            merged_prev
            .groupby("_lu_idx")["_new_eff_ts"]
            .min()
            .rename("_candidate_dv")
        )
        lu = lu.join(min_new_eff, on="_lu_idx")

        has_cand = lu["_candidate_dv"].notna()
        cand_minus1 = lu.loc[has_cand, "_candidate_dv"] - pd.Timedelta(days=1)
        curr = lu.loc[has_cand, _DATE_VALID_COL]

        # min(date_valid_actual, candidato): NaT se ignora en .min(axis=1)
        lu.loc[has_cand, _DATE_VALID_COL] = (
            pd.concat([curr, cand_minus1], axis=1).min(axis=1)
        )
        lu_updated_count = int(has_cand.sum())
        lu = lu.drop(columns=["_candidate_dv"])

    # Paso 2 - date_valid para filas nuevas out-of-order
    df = df.reset_index(drop=True)
    df["_df_idx"] = df.index

    lu_succ = lu[_MATCH_KEYS + ["_eff_ts"]].rename(columns={"_eff_ts": "_lu_eff_ts"})
    merged_next = df[_MATCH_KEYS + ["_df_idx", "_eff_ts"]].merge(
        lu_succ, on=_MATCH_KEYS, how="left"
    )
    merged_next = merged_next[merged_next["_lu_eff_ts"] > merged_next["_eff_ts"]]

    new_rows_with_dv = 0
    if not merged_next.empty:
        min_lu_succ = (
            merged_next
            .groupby("_df_idx")["_lu_eff_ts"]
            .min()
            .rename("_candidate_dv")
        )
        df = df.join(min_lu_succ, on="_df_idx")

        has_cand = df["_candidate_dv"].notna()
        cand_minus1 = df.loc[has_cand, "_candidate_dv"] - pd.Timedelta(days=1)
        curr = df.loc[has_cand, _DATE_VALID_COL]

        df.loc[has_cand, _DATE_VALID_COL] = (
            pd.concat([curr, cand_minus1], axis=1).min(axis=1)
        )
        new_rows_with_dv = int(has_cand.sum())
        df = df.drop(columns=["_candidate_dv"])

    log.logger.info(
        f"{lu_updated_count} fila(s) de lu_ardef actualizadas con date_validad | "
        f"{new_rows_with_dv} fila(s) nuevas recibieron date_valid por out-of-order | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    # Limpiar columnas temporales
    lu = lu.drop(columns=["_eff_ts", "_lu_idx"])
    df_out = df.drop(columns=["_eff_ts", "_df_idx"])

    # Acumular: lu_ardef histórico actualizado + nuevas filas 
    updated_lu_ardef = pd.concat([lu, df_out], ignore_index=True)

    return df_out, updated_lu_ardef


# Funcion publica del modulo

def calculate_ardef(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    file_id: str,
    file_processing_date: str,
    origin_subdir: str = "300_ARDEF_CLN",
    target_subdir: str = "400_ARDEF_CAL",
    operational_subdir: str = _LU_ARDEF_FILENAME,
) -> None:
    """
    Etapa CALCULATE del pipeline ARDEF.

    Pasos:
    1. Leer STAGING / 300_ARDEF_CLN + parquet CLEAN
    2. Leer OPERATIONAL / lu_ardef.parquet acumulativo 
    (puede no existir en la primera ejecucion).
    3. Calcular date_valid con soporte out-of-order:
        - Deduplicacion por 'lines': registros idénticos a los ya en lu_ardef se descartan.
        - Filas nuevas en orden -> date_valid = NaT
        - Filas nuevas retrasadas -> date_valid = eff_sucesor_en_lu - 1 d
        - Filas históricas en lu -> date_valid = min(actual, eff_nueva - 1 d)
    4. Escribir OPERATIONAL / lu_ardef.parquet actualizado
    5. Escribir STAGING / 400_ARDEF_CAL -> parquet CALCULATE (solo las nuevas filas).
    """

    log.logger.info(
        f"Inicio calculate_ardef | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    # 1. Leer CLEAN
    clean = fs.read_parquet(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir,
    )

    log.logger.info(
        f"CLEAN Leido: {len(clean)} filas | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    # 2. Leer todos los parquets OPERATIONAL históricos
    lu_ardef_filepath = fs.get_lu_ardef_filepath(
        file_id=file_id,
        file_processing_date=file_processing_date,
        filename=operational_subdir,
    )

    lu_ardef = _load_lu_ardef(lu_ardef_filepath)

    # 3. Calcular date_valid
    df_calculate, updated_lu_ardef = _build_calculate_dataframe(
        clean=clean,
        lu_ardef=lu_ardef,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    # 4. Escribir lu_ardef.parquet acumulativo actualizado
    try: 
        fs.write_parquet_by_filepath(
            data=updated_lu_ardef,
            filepath=lu_ardef_filepath,
            index=False,
        )

        log.logger.info(
            f"lu_ardef.parquet actualizado: {lu_ardef_filepath} "
            f"({len(updated_lu_ardef)} filas totales)"
        )

    except Exception as exc:
        log.logger.error(
            f"Error al escribir lu_ardef.parquet ({lu_ardef_filepath}: {exc})"
        )

    # 5. Escribir calculate (solo nuevas filas)
    output_filepath = fs.write_parquet(
        data=df_calculate,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False,
    )

    log.logger.info(
        f"ARDEF CALCULATE parquet creado exitosamente: {output_filepath}"
    )
