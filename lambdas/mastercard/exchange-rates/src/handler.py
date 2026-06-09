import io
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# CAMBIO CLAVE: Usamos curl_cffi en lugar del requests estándar para evitar el 403
from curl_cffi import requests

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =============================================================================
# CONFIGURATION
# =============================================================================

S3_BUCKET     = os.environ.get("S3_BUCKET",     "itl-0004-itx-dev-intchg-02-s3-reference")
S3_PREFIX     = os.environ.get("S3_PREFIX",     "exchange-rates/brand=Mastercard")
FUNCTION_NAME = os.environ.get("FUNCTION_NAME", "itl-0004-itx-dev-intchg-02-lmbd-mc-exchange-rates")
# Nota: datetime.utcnow() está deprecado en Python 3.11+, usamos datetime.now(timezone.utc) si es necesario, 
BEGIN_DATE    = os.environ.get("BEGIN_DATE",     datetime.utcnow().strftime("%Y-%m-%d"))
END_DATE      = os.environ.get("END_DATE",       datetime.utcnow().strftime("%Y-%m-%d"))

NUM_CHUNKS      = 5
MAX_WORKERS     = 10
REQUEST_TIMEOUT = 10  # Se aumenta a 10s para dar margen al handshake TLS de curl_cffi
PAUSE_MIN       = 1
PAUSE_MAX       = 1.75

# CABECERAS CORREGIDAS: Simulan una petición real desde el portal de Mastercard
REQUEST_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Referer": "https://www.mastercard.us/en-us/personal/get-support/convert-currency.html",
    "Origin": "https://www.mastercard.us",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin"
}

MASTERCARD_CURRENCIES_URL = (
    "https://www.mastercard.com/settlement/currencyrate/settlement-currencies"
)
MASTERCARD_RATES_URL = (
    "https://www.mastercard.com/marketingservices/public/mccom-services/"
    "currency-conversions/conversion-rates"
)

DATE_FORMAT_INPUT  = "%Y-%m-%d"
DATE_FORMAT_OUTPUT = "%m/%d/%Y"
DATE_FORMAT_FILE   = "%Y%m%d"

# =============================================================================
# HELPERS
# =============================================================================

def generate_date_range(begin_date_str: str, end_date_str: str) -> list[str]:
    """Returns a list of dates in MM/DD/YYYY format between two YYYY-MM-DD dates."""
    try:
        begin = datetime.strptime(begin_date_str, DATE_FORMAT_INPUT)
        end   = datetime.strptime(end_date_str,   DATE_FORMAT_INPUT)
        dates = [
            (begin + timedelta(days=i)).strftime(DATE_FORMAT_OUTPUT)
            for i in range((end - begin).days + 1)
        ]
        logger.info(f"[generate_date_range] {len(dates)} date(s) generated: {dates[0]} -> {dates[-1]}")
        return dates
    except ValueError as e:
        logger.error(f"[generate_date_range] Invalid date format: {e}")
        raise


def split_into_chunks(items: list, num_chunks: int) -> list[list]:
    """Splits a list into N evenly distributed chunks."""
    try:
        chunk_size, remainder = divmod(len(items), num_chunks)
        chunks = []
        start  = 0

        for i in range(num_chunks):
            end = start + chunk_size + (1 if i < remainder else 0)
            chunks.append(items[start:end])
            start = end

        sizes = [len(c) for c in chunks]
        logger.info(
            f"[split_into_chunks] {len(items)} items split into {num_chunks} chunks | "
            f"min={min(sizes)} | max={max(sizes)} | sizes={sizes}"
        )
        return chunks
    except Exception as e:
        logger.error(f"[split_into_chunks] Failed to split list: {e}")
        raise


def delete_existing_parquets(date_str: str) -> int:
    """Deletes all parquet files under the S3 prefix for a given date."""
    prefix = f"{S3_PREFIX}/exchange_date={date_str}/"

    try:
        s3        = boto3.client("s3")
        objects   = []
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            objects.extend(page.get("Contents", []))

        if not objects:
            logger.info(f"[delete_existing_parquets] No existing files found at s3://{S3_BUCKET}/{prefix}")
            return 0

        delete_payload = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        s3.delete_objects(Bucket=S3_BUCKET, Delete=delete_payload)

        logger.info(f"[delete_existing_parquets] Deleted {len(objects)} file(s) from s3://{S3_BUCKET}/{prefix}")
        return len(objects)

    except Exception as e:
        logger.error(f"[delete_existing_parquets] Failed to delete files at {prefix}: {e}")
        raise


def build_s3_key(date_str: str, chunk_id: int) -> str:
    file_date = datetime.strptime(date_str, DATE_FORMAT_INPUT).strftime(DATE_FORMAT_FILE)
    return f"{S3_PREFIX}/exchange_date={date_str}/{file_date}_chunk_{chunk_id}.parquet"


def save_chunk_to_s3(records: list[dict], date_str: str, chunk_id: int) -> str:
    """Serializa registros directamente en un archivo parquet usando pyarrow y lo sube a S3."""
    s3_key        = build_s3_key(date_str, chunk_id)
    valid_records = [r for r in records if r["fx_rate"] != ""]
    skipped_count = len(records) - len(valid_records)

    if not valid_records:
        logger.warning(f"[save_chunk_to_s3] chunk_id={chunk_id} | No valid records to save, skipping upload")
        return s3_key

    try:
        # Creamos la tabla de PyArrow directamente desde las listas de datos
        table = pa.table({
            "date":          [r["date"]          for r in valid_records],
            "from_currency": [r["from_currency"] for r in valid_records],
            "to_currency":   [r["to_currency"]   for r in valid_records],
            "fx_rate":       [r["fx_rate"]       for r in valid_records],
            "brand":         [r["brand"]         for r in valid_records],
        })

        buffer = io.BytesIO()
        # Escribimos la tabla usando pyarrow.parquet
        pq.write_table(table, buffer)
        buffer.seek(0)

        boto3.client("s3").put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        logger.info(
            f"[save_chunk_to_s3] chunk_id={chunk_id} | "
            f"written={len(valid_records)} | skipped={skipped_count} | "
            f"s3://{S3_BUCKET}/{s3_key}"
        )
        return s3_key

    except Exception as e:
        logger.error(f"[save_chunk_to_s3] chunk_id={chunk_id} | Failed to upload parquet: {e}")
        raise




def invoke_next_worker(date: str, chunks: list, chunk_index: int) -> None:
    """Invokes the next worker in the chain asynchronously."""
    if chunk_index >= len(chunks):
        logger.info("[invoke_next_worker] All chunks processed. Chain complete.")
        return

    try:
        payload  = {
            "mode":        "worker",
            "date":        date,
            "chunks":      chunks,
            "chunk_index": chunk_index,
        }
        response = boto3.client("lambda").invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )
        logger.info(
            f"[invoke_next_worker] Invoked worker for chunk_index={chunk_index} "
            f"(chunk_id={chunk_index + 1}/{len(chunks)}) | "
            f"pairs={len(chunks[chunk_index])} | status={response['StatusCode']}"
        )
    except Exception as e:
        logger.error(f"[invoke_next_worker] Failed to invoke worker at chunk_index={chunk_index}: {e}")
        raise

# =============================================================================
# STEP 1: Fetch currency list from Mastercard
# =============================================================================

def fetch_currency_list() -> list[list[str]] | str:
    """Carga la lista de divisas desde un archivo estático para evitar bloqueos del WAF."""
    logger.info("[fetch_currency_list] Loading supported currencies from local JSON...")
    
    try:
        # Crea un archivo 'currencies.json' en la misma carpeta que tu lambda_function.py
        with open("currencies.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        currencies = [c["alphaCd"] for c in data["currencies"]]
        pairs = [
            [src, dst]
            for src in currencies
            for dst in currencies
            if src != dst
        ]

        logger.info(f"[fetch_currency_list] {len(currencies)} currencies -> {len(pairs)} pairs")
        return pairs

    except FileNotFoundError:
        logger.error("[fetch_currency_list] currencies.json file not found in deployment package.")
        return "error"
    except Exception as e:
        logger.error(f"[fetch_currency_list] Unexpected error: {e}")
        return "error"
# =============================================================================
# STEP 2: Fetch exchange rate for a single currency pair
# =============================================================================

def fetch_exchange_rate(
    date: str,
    pair: tuple[str, str],
    index: int,
    total: int,
) -> dict:
    """Fetches exchange rate. """
    from_currency, to_currency = pair
    date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)

    params = {
        "exchange_date":               date_str,
        "transaction_currency":        from_currency,
        "cardholder_billing_currency": to_currency,
        "bank_fee":                    "0",
        "transaction_amount":          "1",
    }


    empty_record = {
        "date":          date_str,
        "from_currency": from_currency,
        "to_currency":   to_currency,
        "fx_rate":       "",
        "brand":         "MASTERCARD",
    }

    try:
        # CAMBIO ESENCIAL: Usamos requests de curl_cffi con impersonate obligatorio
        response = requests.get(
            MASTERCARD_RATES_URL,
            params=params,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT,
            impersonate="chrome120"
        )

        if not response.text.strip():
            logger.warning(
                f"[{index}/{total}] Empty response (possible rate limit) | "
                f"{from_currency}->{to_currency} | {date_str}"
            )
            time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
            return empty_record

        if response.status_code == 403:
            logger.warning(
                f"[{index}/{total}] HTTP 403 (Blocked) | "
                f"{from_currency}->{to_currency} | {date_str}"
            )
            time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
            return empty_record

        if response.status_code != 200:
            logger.warning(
                f"[{index}/{total}] HTTP {response.status_code} | "
                f"{from_currency}->{to_currency} | {date_str}"
            )
            time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
            return empty_record

        fx_rate = float(str(response.json()["data"]["conversionRate"]).replace(",", ""))
        time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))

        logger.info(f"[{index}/{total}] OK {from_currency}->{to_currency} | {date_str} | fx={fx_rate}")
        return {
            "date":          date_str,
            "from_currency": from_currency,
            "to_currency":   to_currency,
            "fx_rate":       fx_rate,
            "brand":         "MASTERCARD",
        }

    except Exception as e:
        logger.error(f"[{index}/{total}] Error/Timeout | {from_currency}->{to_currency} | {date_str} | {e}")

    time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
    return empty_record

# =============================================================================
# ORCHESTRATOR
# =============================================================================

def run_orchestrator(begin_date: str, end_date: str) -> dict:
    logger.info(f"[ORCHESTRATOR] Starting | begin={begin_date} | end={end_date}")

    try:
        dates = generate_date_range(begin_date, end_date)
        pairs = fetch_currency_list()

        if pairs == "error":
            raise RuntimeError("Failed to retrieve currency list from Mastercard")

        chunks = split_into_chunks(pairs, NUM_CHUNKS)

        for date in dates:
            date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)
            #delete_existing_parquets(date_str)

            logger.info(f"[ORCHESTRATOR] Starting chain for {date} | {NUM_CHUNKS} chunks | {len(pairs)} pairs...")
            invoke_next_worker(date, chunks, chunk_index=0)

        logger.info(f"[ORCHESTRATOR] Done | {len(dates)} chain(s) started | {len(pairs)} total pairs")
        return {
            "statusCode":  200,
            "mode":        "orchestrator",
            "chains":      len(dates),
            "total_pairs": len(pairs),
            "dates":       dates,
        }

    except Exception as e:
        logger.error(f"[ORCHESTRATOR] Fatal error: {e}")
        raise

# =============================================================================
# WORKER
# =============================================================================

def run_worker(date: str, chunks: list, chunk_index: int) -> dict:
    chunk_id = chunk_index + 1
    pairs    = chunks[chunk_index]

    logger.info(
        f"[WORKER {chunk_id}/{len(chunks)}] Starting | "
        f"date={date} | pairs={len(pairs)} | chunk_index={chunk_index}"
    )

    try:
        date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)
        total    = len(pairs)
        results  = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    fetch_exchange_rate,
                    date,
                    tuple(pair),
                    index + 1,
                    total,
                ): index
                for index, pair in enumerate(pairs)
            }
            completed_count = 0
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                    completed_count += 1
                    if completed_count % 500 == 0:
                        logger.info(
                            f"[WORKER {chunk_id}/{len(chunks)}] Progress | "
                            f"completed={completed_count}/{total} | "
                            f"({round(completed_count / total * 100, 1)}%)"
                        )
                except Exception as e:
                    logger.error(f"[WORKER {chunk_id}/{len(chunks)}] Error retrieving thread result: {e}")

        s3_key        = save_chunk_to_s3(results, date_str, chunk_id)
        written_count = len([r for r in results if r["fx_rate"] != ""])
        skipped_count = len(results) - written_count

        logger.info(
            f"[WORKER {chunk_id}/{len(chunks)}] Done | date={date_str} | "
            f"written={written_count} | skipped={skipped_count} | file={s3_key}"
        )

        invoke_next_worker(date, chunks, chunk_index=chunk_index + 1)

        return {
            "statusCode":   200,
            "mode":         "worker",
            "chunk_id":     chunk_id,
            "records_ok":   written_count,
            "records_skip": skipped_count,
            "s3_key":       s3_key,
        }

    except Exception as e:
        logger.error(f"[WORKER {chunk_id}/{len(chunks)}] Fatal error | date={date} | {e}")
        raise

# =============================================================================
# MAIN HANDLER
# =============================================================================

def lambda_handler(event: dict, context) -> dict:
    #logger.info(f"[lambda_handler] RAW EVENT: {json.dumps(event)}")
    mode = event.get("mode", "orchestrator")
    logger.info(f"[lambda_handler] Event received | mode={mode}")

    try:
        if mode == "orchestrator":
            begin_date = event.get("begin_date", BEGIN_DATE)
            end_date   = event.get("end_date",   END_DATE)
            return run_orchestrator(begin_date, end_date)

        if mode == "worker":
            date        = event["date"]
            chunks      = event["chunks"]
            chunk_index = event.get("chunk_index", 0)
            return run_worker(date, chunks, chunk_index)

        raise ValueError(f"Unknown mode: '{mode}'. Use 'orchestrator' or 'worker'.")

    except KeyError as e:
        logger.error(f"[lambda_handler] Missing required field in event: {e}")
        raise
    except ValueError as e:
        logger.error(f"[lambda_handler] Invalid event value: {e}")
        raise
    except Exception as e:
        logger.error(f"[lambda_handler] Fatal error: {e}")
        raise