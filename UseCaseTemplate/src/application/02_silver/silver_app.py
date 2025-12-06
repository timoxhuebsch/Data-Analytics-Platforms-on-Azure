# ---------Transformation Pipeline from Bronze to Silver ---------

# ---------------------------- IMPORT ----------------------------
import sys
import os
import json
import logging
import hashlib
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
import zoneinfo

import pandas as pd
from pandas import DataFrame
from azure.identity import InteractiveBrowserCredential

# utils import (Projekt-Layout)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from application.utils.auth import get_keyvault_secrets
from application.utils.io import read_parquet_pandas_df, write_parquet_pandas_df, read_csv_pandas_df

# ---------- CONFIG ----------
ENV = 'dt'
RUN_ENVIRONMENT = 'vs_code'
VALID_RUN_ENVIRONMENTS = {'vs_code', 'synapse'}

if RUN_ENVIRONMENT not in VALID_RUN_ENVIRONMENTS:
    raise ValueError(f"Invalid value for RUN_ENVIRONMENT: {RUN_ENVIRONMENT}. Must be one of {VALID_RUN_ENVIRONMENTS}.")

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------- Helper-Funktionen ----------
def load_config(env: str) -> Dict[str, Any]:
    cfg_path = os.path.join(os.path.dirname(__file__), f'config/{env}/silver_config.json')
    with open(cfg_path, 'r', encoding='utf-8') as fh:
        return json.load(fh)


def get_storage_secret(keyvault_name: str, secret_name: str, run_environment: str) -> str:
    credential = InteractiveBrowserCredential(additionally_allowed_tenants="*")
    return get_keyvault_secrets(
        keyvault_name=keyvault_name,
        run_environment=run_environment,
        secret_name=secret_name,
        credential=credential,
    )


def read_all_sources(files_cfg: List[Dict[str, Any]], source_cfg: Dict[str, Any], storage_secret: str) -> List[DataFrame]:
    dfs: List[DataFrame] = []
    for file_cfg in files_cfg:
        src_file = file_cfg["source_file"]
        log.info("Reading source file: %s", src_file)
        read_opts = file_cfg.get("read_options", {})
        file_type = read_opts.get("file_type", "parquet").lower()
        if file_type == "parquet":
            df = read_parquet_pandas_df(
                storage_account_name=source_cfg["storage_account_name"],
                storage_account_secret=storage_secret,
                container_name=source_cfg["container_name"],
                file_path=src_file
            )
        elif file_type == "csv":
            df = read_csv_pandas_df(
                storage_account_name=source_cfg["storage_account_name"],
                storage_account_secret=storage_secret,
                container_name=source_cfg["container_name"],
                file_path=src_file
            )
        else:
            raise ValueError(f"Unsupported file_type '{file_type}' for {src_file}")
        dfs.append(df)
    return dfs


def add_ingestion_time_to_dfs(dfs: List[DataFrame], tz_name: str = "Europe/Berlin") -> None:
    tz = zoneinfo.ZoneInfo(tz_name)
    ingestion_time = datetime.now(tz).isoformat()
    for df in dfs:
        df['_ingestion_time'] = ingestion_time


def add_source_column(dfs: List[DataFrame], files_cfg: List[Dict[str, Any]]) -> None:
    for df, file_cfg in zip(dfs, files_cfg):
        source_name = file_cfg.get("source_name") or file_cfg.get("source_url") or "unknown_source"
        df['_source_name'] = source_name
        if "source_url" in file_cfg:
            df['_source_url'] = file_cfg.get("source_url")


# ---------------- Silver-specific transformations ----------------

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Wandelt alle Spaltennamen in einheitliches Format:
    - Leerzeichen durch Unterstrich ersetzen
    - Alle Buchstaben bleiben unverändert (oder optional lowercase)
    - Entfernt evtl. führende/trailing Whitespaces
    """
    df = df.rename(columns=lambda x: x.strip().replace(" ", "_"))
    return df

def _trim_string_columns(df: DataFrame) -> None:
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for c in str_cols:
        df[c] = df[c].astype("string").str.strip()


def _convert_numeric_columns(df: DataFrame) -> None:
    for col in df.columns:
        if col.startswith("_"):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        series = df[col].dropna().astype(str)
        if len(series) == 0:
            continue
        numeric_like = series.str.match(r"^-?\d+(\.\d+)?$").sum() / len(series)
        if numeric_like >= 0.9:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def _convert_datetime_columns(df: DataFrame) -> None:
    for col in df.columns:
        if col.startswith("_"):
            continue
        lname = col.lower()
        # Only treat columns that clearly indicate a date or timestamp
        if "date" in lname or "timestamp" in lname:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

def compute_row_hash(series: pd.Series, cols: List[str]) -> str:
    concat = "|".join("" if pd.isna(series[c]) else str(series[c]) for c in cols)
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()


def add_row_hash_and_record_id(df: DataFrame, hash_cols: Optional[List[str]] = None) -> None:
    if 'row_hash' in df.columns and 'record_id' in df.columns:
        return
    if hash_cols is None:
        hash_cols = [c for c in df.columns if not str(c).startswith("_")]
    hash_cols = sorted(hash_cols)
    df['_row_hash'] = df.apply(lambda r: compute_row_hash(r, hash_cols), axis=1)
    df['_record_id'] = [str(uuid.uuid4()) for _ in range(len(df))]


def deduplicate_df_by_row_hash(df: DataFrame) -> DataFrame:
    if 'row_hash' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['row_hash'])
        log.info("Dropped %d duplicate rows (by row_hash).", before - len(df))
    else:
        before = len(df)
        df = df.drop_duplicates()
        log.info("Dropped %d duplicate rows (by full-row).", before - len(df))
    return df


# ---- New: Primary key helpers ----

def apply_primary_key_rules(df: DataFrame, primary_keys: List[str]) -> DataFrame:
    """
    1) Remove rows where any existing primary key is null
    2) Deduplicate by existing primary_keys (keep first)
    If none of the configured primary_keys exist in df, function returns df unchanged (no drop).
    """
    if not primary_keys:
        return df

    # determine which of the configured keys actually exist in the dataframe
    existing_keys = [k for k in primary_keys if k in df.columns]
    missing_keys = [k for k in primary_keys if k not in df.columns]
    if missing_keys:
        log.warning("Primary key columns not found in DataFrame and will be ignored: %s", missing_keys)

    # if no configured primary keys exist in this df, skip primary key rules
    if not existing_keys:
        log.info("No primary key columns present in DataFrame; skipping primary-key based cleaning.")
        return df

    # Count and drop rows that have NULL in any of the existing primary keys
    null_mask = pd.Series(False, index=df.index)
    for k in existing_keys:
        null_mask = null_mask | df[k].isna()

    null_count = int(null_mask.sum())
    if null_count > 0:
        log.info("Dropping %d rows with NULL in primary keys %s", null_count, existing_keys)
        df = df.loc[~null_mask].copy()

    # Deduplicate by the existing primary keys
    before = len(df)
    df = df.drop_duplicates(subset=existing_keys)
    dropped = before - len(df)
    if dropped > 0:
        log.info("Dropped %d duplicate rows based on primary keys %s", dropped, existing_keys)

    return df


# ---- QC ----

def qc_checks(df: DataFrame, required_columns: Optional[List[str]] = None) -> Dict[str, Any]:
    result = {}
    result['row_count'] = len(df)
    result['null_counts'] = df.isna().sum().to_dict()
    if required_columns:
        missing = [c for c in required_columns if c not in df.columns]
        result['missing_columns'] = missing
        req_nulls = {c: int(df[c].isna().sum()) for c in required_columns if c in df.columns}
        result['required_nulls'] = req_nulls
        result['required_pass'] = all((c in df.columns and int(df[c].isna().sum()) == 0) for c in required_columns)
    else:
        result['missing_columns'] = []
        result['required_nulls'] = {}
        result['required_pass'] = True
    return result


def transform_for_silver(df: DataFrame, file_cfg: Dict[str, Any], config: Dict[str, Any]) -> DataFrame:
    df = df.copy()

    # 1) Trim strings
    _trim_string_columns(df)

    # 2) Convert numerics & datetimes heuristically
    _convert_numeric_columns(df)
    _convert_datetime_columns(df)

    # 3) Ensure provenance columns
    if '_ingestion_time' not in df.columns:
        tz = zoneinfo.ZoneInfo("Europe/Berlin")
        df['_ingestion_time'] = datetime.now(tz).isoformat()
    if '_source_name' not in df.columns:
        src = file_cfg.get("source_name") or file_cfg.get("source_url") or "unknown_source"
        df['_source_name'] = src

    # 4) Add row_hash and record_id if not present
    add_row_hash_and_record_id(df)

    # 5) If primary keys specified, apply primary key rules BEFORE row-hash dedupe
    primary_keys = file_cfg.get("primary_keys", [])
    if primary_keys:
        df = apply_primary_key_rules(df, primary_keys)

    # 6) Deduplicate by row_hash as final fallback
    df = deduplicate_df_by_row_hash(df)

    # 7) QC checks
    required_columns = config.get("silver", {}).get("required_columns")
    qc = qc_checks(df, required_columns=required_columns)
    log.info("QC for %s: rows=%d, required_pass=%s", file_cfg.get("source_file"), qc['row_count'], qc['required_pass'])
    if not qc['required_pass']:
        log.error("QC failed for %s: %s", file_cfg.get("source_file"), qc)
        raise RuntimeError(f"QC failed for {file_cfg.get('source_file')}: {qc}")

    # 8) Optional partition date
    if config.get("destination", {}).get("partition_by_ingestion_date", False):
        df['partition_date'] = pd.to_datetime(df['_ingestion_time']).dt.date

    return df


def write_all_dfs(dfs: List[DataFrame], files_cfg: List[Dict[str, Any]], dest_cfg: Dict[str, Any], storage_secret: str) -> None:
    for file_cfg, df in zip(files_cfg, dfs):
        target_file = file_cfg["target_file"]
        current_file_path = dest_cfg.get("file_path", "") + target_file
        log.info("Writing %s -> %s", target_file, current_file_path)

        write_parquet_pandas_df(
            data=df,
            storage_account_name=dest_cfg["storage_account_name"],
            storage_account_secret=storage_secret,
            container_name=dest_cfg["container_name"],
            file_path=current_file_path,
        )

        print(
            f"Parquet file {current_file_path} successfully written to "
            f"{dest_cfg['container_name']}/{current_file_path} in "
            f"{dest_cfg['storage_account_name']}."
        )


# ---------- main ----------
def main() -> None:
    config = load_config(ENV)

    keyvault_name = config.get("keyvault_name")
    if not keyvault_name:
        raise ValueError("keyvault_name missing in config")

    files_to_process = config.get("files_to_process", [])

    for file_cfg in files_to_process:
        if "primary_keys" in file_cfg and isinstance(file_cfg["primary_keys"], list):
            file_cfg["primary_keys"] = [k.strip().replace(" ", "_") for k in file_cfg["primary_keys"]]

    if not files_to_process:
        log.warning("No files_to_process found in config, exiting.")
        return

    storage_secret = get_storage_secret(
        keyvault_name=keyvault_name,
        secret_name=config["source"]["storage_account_secret_name"],
        run_environment=RUN_ENVIRONMENT
    )

    # 1) Read Bronze
    dataframes = read_all_sources(files_to_process, config["source"], storage_secret)

    # 2) Add provenance
    dataframes = [normalize_column_names(df) for df in dataframes]
    add_ingestion_time_to_dfs(dataframes, tz_name="Europe/Berlin")
    add_source_column(dataframes, files_to_process)

    # 3) Transform each df for Silver (includes primary key handling)
    transformed_dfs: List[DataFrame] = []
    for df, file_cfg in zip(dataframes, files_to_process):
        log.info("Transforming %s for Silver", file_cfg.get("source_file"))
        transformed = transform_for_silver(df, file_cfg, config)
        transformed_dfs.append(transformed)

    # 4) Write Silver
    write_all_dfs(transformed_dfs, files_to_process, config["destination"], storage_secret)


if __name__ == "__main__":
    main()
