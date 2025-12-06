# ---------------------------- IMPORT ----------------------------
import sys
import os
import json
from azure.identity import InteractiveBrowserCredential
import pandas as pd

# sys.path to be able to read functions from utils folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from application.utils.auth import get_keyvault_secrets
from application.utils.io import read_parquet_pandas_df


# 1 ---------------------------- VARIABLES ----------------------------
# Variable for environment
env = 'dt'

# Variable for environment of run execution
run_environment = 'vs_code'
if run_environment not in ['vs_code', 'synapse']:
    raise ValueError(f"Invalid value for run_environment: {run_environment}. Must be 'vs_code' or 'synapse'.")

# Variable for KeyVault Name which contains the secret for the Storage Account
keyvault_name = f"kvdataanalyticsth"


# 2 ---------------------------- CONFIGURATION FILES ----------------------------
# configuration files contain information about source and destination of data
config_path_src = os.path.join(os.path.dirname(__file__), f'config/{env}/src.json')
with open(config_path_src, 'r') as config_file:
    src_config = json.load(config_file)


# 3 ---------------------------- AUTHENTICATION ----------------------------
credential = InteractiveBrowserCredential(additionally_allowed_tenants="*")

# Log in with your Azure Account to receive stored secret in KeyVault 
src_storage_account_secret = get_keyvault_secrets(
    keyvault_name = keyvault_name, 
    run_environment = run_environment,
    secret_name = src_config.get('storage_account_secret_name'), 
    credential = credential,
)


# 4 ---------------------------- READ DATA FROM SOURCE ----------------------------
df = read_parquet_pandas_df(
            storage_account_name = src_config.get('storage_account_name'), 
            storage_account_secret = src_storage_account_secret, 
            container_name = src_config.get('container_name'), 
            file_path = src_config.get('file_path'),
            )
df.to_csv("preview.csv", index=False)
#print(df.head(5))


# 5 ---------------------------- TRANSFORM DATA ----------------------------


# 6 ---------------------------- WRITE DATA ----------------------------
