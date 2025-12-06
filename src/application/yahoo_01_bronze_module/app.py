# ---------------------------- IMPORT ----------------------------
import sys
import os
import requests
import json
from azure.identity import InteractiveBrowserCredential

# sys.path to be able to read functions from utils folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from application.utils.io import write_json
from application.utils.auth import get_keyvault_secrets


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

config_path_dstn = os.path.join(os.path.dirname(__file__), f'config/{env}/dstn_adls.json')
with open(config_path_dstn, 'r') as config_file:
    dstn_config = json.load(config_file)

# Yahoo Finance API Base URL - "https://query1.finance.yahoo.com/v8/finance/chart/"
api_base_url = src_config.get('api_base_url')


# 3 ---------------------------- AUTHENTICATION ----------------------------
credential = InteractiveBrowserCredential(additionally_allowed_tenants="*")

# Log in with your Azure Account to receive stored secret in KeyVault 
dstn_storage_account_secret = get_keyvault_secrets(
    keyvault_name = keyvault_name, 
    run_environment = run_environment,
    secret_name = dstn_config.get('storage_account_secret_name'), 
    credential = credential,
)


# 4 ---------------------------- READ DATA FROM SOURCE ----------------------------
ticker_lst = [
    'AAPL',             # Apple
    'NVDA',             # Nvidia
    'MSFT',             # Microsoft
    'META',             # Meta
    'AMZN',             # Amazon
    'SXR8.DE'           # S&P 500 ETF
]
for ticker in ticker_lst:
    print(f'Processing {ticker} data.')
    current_api_url = api_base_url + f'{ticker}'
    params = {
        'range': src_config.get('period'),
        'interval': src_config.get('interval'),
    }
    # common user agent string that simulates a modern web browser
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }

# 5 ---------------------------- TRANSFORM DATA ----------------------------
    response = requests.get(current_api_url, params=params, headers=headers)
    data = response.json()
    print(data)

    if 'chart' not in data or 'result' not in data['chart']:
        raise Exception(f"Failed to fetch data for {ticker}. Response: {data}")

# 6 ---------------------------- WRITE DATA ----------------------------
    current_file_path = dstn_config.get('file_path') + f'{ticker}_raw.json'
    write_json(
        data = data,
        storage_account_name = dstn_config.get('storage_account_name'), 
        storage_account_secret = dstn_storage_account_secret, 
        container_name = dstn_config.get('container_name'), 
        file_path = current_file_path,
        )

    print(
        f"JSON file for {ticker} successfully written to {dstn_config.get('container_name')}/{current_file_path} in {dstn_config.get('storage_account_name')}."
    )