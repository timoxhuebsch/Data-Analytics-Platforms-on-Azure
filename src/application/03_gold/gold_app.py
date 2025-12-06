#!/usr/bin/env python3
# gold/app.py - aggregate Silver -> Gold by Age & Gender (Pandas)

import sys
import os
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import zoneinfo

import numpy as np
import pandas as pd
from pandas import DataFrame
from azure.identity import InteractiveBrowserCredential

# project utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from application.utils.auth import get_keyvault_secrets
from application.utils.io import read_parquet_pandas_df, write_parquet_pandas_df, read_csv_pandas_df

# ---------- config ----------
ENV = 'dt'
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def load_config(env: str) -> Dict[str, Any]:
    cfg_path = os.path.join(os.path.dirname(__file__), f'config/{env}/gold_config.json')
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

def read_source_file(file_cfg: Dict[str,Any], source_cfg: Dict[str,Any], storage_secret: str) -> DataFrame:
    src = file_cfg["source_file"]
    read_opts = file_cfg.get("read_options", {})
    ftype = read_opts.get("file_type", "parquet").lower()
    LOG.info("Reading %s (type=%s)", src, ftype)
    if ftype == "parquet":
        return read_parquet_pandas_df(
            storage_account_name=source_cfg["storage_account_name"],
            storage_account_secret=storage_secret,
            container_name=source_cfg["container_name"],
            file_path=src
        )
    elif ftype == "csv":
        return read_csv_pandas_df(
            storage_account_name=source_cfg["storage_account_name"],
            storage_account_secret=storage_secret,
            container_name=source_cfg["container_name"],
            file_path=src
        )
    else:
        raise ValueError("Unsupported file type: " + ftype)

def sanitize_keys(df: DataFrame, join_keys: List[str]) -> DataFrame:
    """Ensure join columns exist and have consistent dtypes (Age -> int, Gender -> string)."""
    for k in join_keys:
        if k not in df.columns:
            LOG.warning("%s not in DataFrame columns; creating with NaN", k)
            df[k] = pd.NA

    # Age -> int (nullable), coerce errors to NaN
    if "Age" in join_keys:
        df["Age"] = pd.to_numeric(df["Age"], errors="coerce").astype("Int64")
    if "Gender" in join_keys:
        df["Gender"] = df["Gender"].astype("string").str.strip().str.capitalize()  # normalize

    return df

def aggregate_by_group(df: DataFrame, join_keys: List[str], numeric_aggs: List[str]=["mean"],
                       config_numeric_columns: Optional[List[str]] = None) -> DataFrame:
    """
    Aggregates numeric columns by join_keys.
    If config_numeric_columns is provided, use that list (intersection with df.columns).
    Otherwise fall back to dtype-based heuristic.
    """
    # choose numeric columns (exclude metadata starting with underscore and join keys)
    if config_numeric_columns:
        # Only keep those numeric_columns that actually exist in df
        num_cols = [c for c in config_numeric_columns if c in df.columns and c not in join_keys and not str(c).startswith("_")]
    else:
        num_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c not in join_keys and not str(c).startswith("_")]

    if not num_cols:
        LOG.warning("No numeric columns found to aggregate for grouping %s", join_keys)

    grp = df.groupby(join_keys)
    # group counts
    counts = grp.size().rename("group_count")
    agg_df = pd.DataFrame(index=counts.index)
    agg_df["group_count"] = counts

    # add aggregated metrics (compute with pandas)
    for col in num_cols:
        for agg in numeric_aggs:
            col_name = f"{col}_{agg}"
            try:
                if agg == "mean":
                    agg_df[col_name] = grp[col].mean()
                elif agg == "median":
                    agg_df[col_name] = grp[col].median()
                elif agg == "min":
                    agg_df[col_name] = grp[col].min()
                elif agg == "max":
                    agg_df[col_name] = grp[col].max()
                elif agg == "std":
                    agg_df[col_name] = grp[col].std()
                else:
                    agg_df[col_name] = grp[col].agg(agg)
            except Exception as e:
                LOG.warning("Aggregation %s on %s failed: %s", agg, col, e)
                agg_df[col_name] = pd.NA

    agg_df = agg_df.reset_index()
    agg_df.columns = [str(c) for c in agg_df.columns]
    return agg_df

def merge_aggregates(agg1: DataFrame, agg2: DataFrame, join_keys: List[str], how: str="inner") -> DataFrame:
    LOG.info("Merging aggregates on %s (how=%s)", join_keys, how)
    merged = pd.merge(agg1, agg2, on=join_keys, how=how, suffixes=("_src1","_src2"))
    return merged

def write_parquet_local_or_adls(df: DataFrame, dest_cfg: Dict[str,Any], file_path: str, storage_secret: str) -> None:
    # wrapper for your io util
    write_parquet_pandas_df(
        data=df,
        storage_account_name=dest_cfg["storage_account_name"],
        storage_account_secret=storage_secret,
        container_name=dest_cfg["container_name"],
        file_path=file_path
    )

def main():
    config = load_config(ENV)
    join_keys = config.get("join_keys", ["Age","Gender"])
    run_env = "vs_code"

    keyvault_name = config.get("keyvault_name")
    if not keyvault_name:
        raise RuntimeError("keyvault_name missing in config")

    storage_secret = get_storage_secret(keyvault_name, config["source"]["storage_account_secret_name"], run_env)
    files = config.get("files_to_process", [])
    if len(files) < 2:
        LOG.warning("Expect 2+ source files; found %d", len(files))

    # read & aggregate each source
    aggregated_frames = []
    for file_cfg in files:
        df = read_source_file(file_cfg, config["source"], storage_secret)
        df = sanitize_keys(df, join_keys)
        # optional: drop rows where Age or Gender is NaN before aggregating (config could control)
        df = df.dropna(subset=join_keys, how="any")
        numeric_aggs = config.get("options", {}).get("numeric_agg", ["mean"])
        config_numeric_cols = config.get("options", {}).get("numeric_columns", None)
        agg_df = aggregate_by_group(df, join_keys, numeric_aggs, config_numeric_columns=config_numeric_cols)
        # write per-source aggregated file (optional)
        out_agg_target = file_cfg.get("aggregated_target_file")
        if out_agg_target:
            LOG.info("Writing aggregated source file %s", out_agg_target)
            write_parquet_local_or_adls(agg_df, config["destination"], out_agg_target, storage_secret)
        aggregated_frames.append(agg_df)

    # merge aggregated frames pairwise (we'll merge all into one; simple left fold)
    merged = aggregated_frames[0]
    for other in aggregated_frames[1:]:
        merged = merge_aggregates(merged, other, join_keys, how=config.get("options", {}).get("join_how", "inner"))

    # final write
    gold_target = config.get("gold_merge_target_file", "gold_aggregated.parquet")
    LOG.info("Writing final gold file: %s", gold_target)
    write_parquet_local_or_adls(merged, config["destination"], gold_target, storage_secret)
    LOG.info("Gold layer write complete. Rows in gold: %d", len(merged))

if __name__ == "__main__":
    main()
