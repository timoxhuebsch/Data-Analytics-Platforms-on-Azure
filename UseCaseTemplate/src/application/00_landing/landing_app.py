"""
Data Pipeline Script - Downloads files from URLs and uploads to Azure Storage
"""
import sys
import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import requests
import pandas as pd
from azure.identity import InteractiveBrowserCredential

# Add utils to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from application.utils.io import write_csv_pandas_df
from application.utils.auth import get_keyvault_secrets

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ---------------------------- CONFIGURATION ----------------------------

class Config:
    """Configuration manager for the data pipeline"""
    
    VALID_ENVIRONMENTS = ['vs_code', 'synapse']
    
    def __init__(self, env: str, run_environment: str):
        self.env = env
        self.run_environment = run_environment
        self._validate_run_environment()
        self.config = self._load_config()
        
    def _validate_run_environment(self) -> None:
        """Validate the run environment"""
        if self.run_environment not in self.VALID_ENVIRONMENTS:
            raise ValueError(
                f"Invalid run_environment: {self.run_environment}. "
                f"Must be one of {self.VALID_ENVIRONMENTS}"
            )
    
    def _load_config(self) -> Dict:
        """Load configuration from JSON file"""
        config_path = Path(__file__).parent / f'config/{self.env}/config.json'
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            return json.load(f)
    
    @property
    def keyvault_name(self) -> str:
        return self.config.get("keyvault_name")
    
    @property
    def sources(self) -> List[Dict]:
        return self.config["sources"]
    
    @property
    def destination(self) -> Dict:
        return self.config["dstn"]


# ---------------------------- DATA DOWNLOAD ----------------------------

def download_file(url: str, target_path: Path) -> None:
    """
    Download a file from URL to target path
    
    Args:
        url: URL to download from
        target_path: Path where file should be saved
        
    Raises:
        requests.HTTPError: If download fails
    """
    try:
        logger.info(f"Downloading from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(target_path, "wb") as f:
            f.write(response.content)
        
        logger.info(f"Successfully downloaded to {target_path}")
        
    except requests.RequestException as e:
        logger.error(f"Failed to download {url}: {e}")
        raise


def download_all_sources(sources: List[Dict], tmp_folder: Path) -> List[Tuple[str, Path]]:
    """
    Download all source files
    
    Args:
        sources: List of source configurations with 'url' and 'file_name'
        tmp_folder: Temporary folder for downloads
        
    Returns:
        List of tuples (file_name, file_path)
    """
    downloaded_files = []
    
    for source in sources:
        url = source["url"]
        file_name = source["file_name"]
        target_path = tmp_folder / file_name
        
        download_file(url, target_path)
        downloaded_files.append((file_name, target_path))
    
    return downloaded_files


# ---------------------------- DATA UPLOAD ----------------------------

def upload_to_azure(
    file_path: Path,
    file_name: str,
    config: Config,
    storage_secret: str
) -> None:
    """
    Upload CSV file to Azure Storage
    
    Args:
        file_path: Local path of file to upload
        file_name: Name of file in Azure Storage
        config: Configuration object
        storage_secret: Azure Storage account secret
    """
    try:
        logger.info(f"Reading CSV from {file_path}")
        data = pd.read_csv(file_path)
        
        dest = config.destination
        azure_file_path = dest["file_path"] + file_name
        
        logger.info(f"Uploading to {azure_file_path}")
        write_csv_pandas_df(
            data=data,
            storage_account_name=dest["storage_account_name"],
            storage_account_secret=storage_secret,
            container_name=dest["container_name"],
            file_path=azure_file_path,
        )
        
        logger.info(
            f"Successfully uploaded {file_name} to "
            f"{dest['container_name']}/{azure_file_path}"
        )
        
    except Exception as e:
        logger.error(f"Failed to upload {file_name}: {e}")
        raise


def process_and_upload_files(
    downloaded_files: List[Tuple[str, Path]],
    config: Config,
    storage_secret: str
) -> None:
    """
    Process and upload all downloaded files to Azure
    
    Args:
        downloaded_files: List of (file_name, file_path) tuples
        config: Configuration object
        storage_secret: Azure Storage account secret
    """
    for file_name, file_path in downloaded_files:
        upload_to_azure(file_path, file_name, config, storage_secret)


# ---------------------------- AUTHENTICATION ----------------------------

def get_azure_credentials(config: Config) -> Tuple[InteractiveBrowserCredential, str]:
    """
    Authenticate with Azure and retrieve storage secret
    
    Args:
        config: Configuration object
        
    Returns:
        Tuple of (credential, storage_secret)
    """
    logger.info("Authenticating with Azure")
    credential = InteractiveBrowserCredential(additionally_allowed_tenants="*")
    
    storage_secret = get_keyvault_secrets(
        keyvault_name=config.keyvault_name,
        run_environment=config.run_environment,
        secret_name=config.destination["storage_account_secret_name"],
        credential=credential,
    )
    
    logger.info("Successfully authenticated")
    return credential, storage_secret


# ---------------------------- MAIN ----------------------------

def main():
    """Main execution function"""
    try:
        # Configuration
        ENV = 'dt'
        RUN_ENVIRONMENT = 'vs_code'
        
        logger.info("Starting data pipeline")
        config = Config(ENV, RUN_ENVIRONMENT)
        
        # Authentication
        _, storage_secret = get_azure_credentials(config)
        
        # Setup paths
        project_root = Path(__file__).parent
        tmp_folder = project_root / "tmp"
        
        # Download files
        logger.info(f"Downloading {len(config.sources)} files")
        downloaded_files = download_all_sources(config.sources, tmp_folder)
        
        # Upload to Azure
        logger.info(f"Uploading {len(downloaded_files)} files to Azure")
        process_and_upload_files(downloaded_files, config, storage_secret)
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()