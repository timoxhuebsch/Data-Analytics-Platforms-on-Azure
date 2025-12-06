# ---------Transformation Pipeline from Landing to Bronze --------

# ---------------------------- IMPORT ----------------------------
import sys
import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime
import zoneinfo
from azure.identity import InteractiveBrowserCredential
from pandas import DataFrame

# utils import (Projekt-Layout)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from application.utils.auth import get_keyvault_secrets
from application.utils.io import read_csv_pandas_df, write_parquet_pandas_df


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
    cfg_path = os.path.join(os.path.dirname(__file__), f'config/{env}/bronze_config.json')
    with open(cfg_path, 'r', encoding='utf-8') as fh:
        return json.load(fh)


def get_storage_secret(keyvault_name: str, secret_name: str, run_environment: str):
    """
    Holt den Secret-Wert aus KeyVault (den tatsächlichen Storage Account Key).
    """
    credential = InteractiveBrowserCredential(additionally_allowed_tenants="*")
    return get_keyvault_secrets(
        keyvault_name=keyvault_name,
        run_environment=run_environment,
        secret_name=secret_name,
        credential=credential,
    )


def read_all_sources(files_cfg: List[Dict[str, Any]], source_cfg: Dict[str, Any], storage_secret: str) -> List[DataFrame]:
    """
    Liest alle in files_cfg definierten Quelldateien in eine Liste von DataFrames.
    Reihenfolge bleibt erhalten.
    """
    dfs: List[DataFrame] = []
    for file_cfg in files_cfg:
        src_file = file_cfg["source_file"]
        log.info("Reading source file: %s", src_file)
        df = read_csv_pandas_df(
            storage_account_name=source_cfg["storage_account_name"],
            storage_account_secret=storage_secret,
            container_name=source_cfg["container_name"],
            file_path=src_file
        )
        dfs.append(df)
    return dfs


def add_ingestion_time_to_dfs(dfs: List[DataFrame], tz_name: str = "Europe/Berlin") -> None:
    """
    Fügt jedem DataFrame die Spalte '_ingestion_time' mit der aktuellen Timezone-Zeit hinzu.
    (In-place)
    """
    tz = zoneinfo.ZoneInfo(tz_name)
    ingestion_time = datetime.now(tz).isoformat()
    for df in dfs:
        df['_ingestion_time'] = ingestion_time

def add_source_column(dfs: List[DataFrame], files_cfg: List[Dict[str, Any]]) -> None:
    """
    Fügt jedem DataFrame eine Spalte '_source_name' hinzu.
    Erwartet, dass in files_cfg für jede Datei ein Feld 'source_name' existiert.
    """
    for df, file_cfg in zip(dfs, files_cfg):
        source_name = file_cfg.get("source_url", "unknown_source")
        df['_source_name'] = source_name

def write_all_dfs(dfs: List[DataFrame], files_cfg: List[Dict[str, Any]], dest_cfg: Dict[str, Any], storage_secret: str) -> None:
    """
    Schreibt alle DataFrames als Parquet in das Ziel, dabei wird files_cfg parallel zu dfs erwartet.
    """
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

        # identische Ausgabe wie vorher
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
    if not files_to_process:
        log.warning("No files_to_process found in config, exiting.")
        return

    # Secret einmal holen (wie vorher: Secret-Name aus source verwenden)
    storage_secret = get_storage_secret(
        keyvault_name=keyvault_name,
        secret_name=config["source"]["storage_account_secret_name"],
        run_environment=RUN_ENVIRONMENT
    )

    # 1) Lesen
    dataframes = read_all_sources(files_to_process, config["source"], storage_secret)

    # 2) Transform (add ingestion time)
    add_ingestion_time_to_dfs(dataframes, tz_name="Europe/Berlin")
    add_source_column(dataframes, files_to_process)

    # 3) Schreiben (Ziel-Konfiguration aus config["destination"])
    write_all_dfs(dataframes, files_to_process, config["destination"], storage_secret)


if __name__ == "__main__":
    main()
