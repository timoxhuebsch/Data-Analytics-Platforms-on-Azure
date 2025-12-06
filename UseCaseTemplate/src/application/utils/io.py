import pandas as pd
import json
from io import BytesIO, StringIO
from azure.storage.blob import BlobServiceClient, BlobClient
from typing import Any, Dict


def get_adls_blob_client(
        storage_account_name: str, 
        storage_account_secret: str, 
        container_name: str, 
        file_path: str
    ) -> BlobClient:
    """
    Create and return a BlobClient for accessing a specific blob in Azure Data Lake Storage (ADLS).

    This function initializes a connection to an Azure Blob Storage account using the provided credentials,
    and returns a `BlobClient` object for the specified blob in the given container.

    Parameters:
    -----------
    storage_account_name : str
        The name of the Azure Storage account.
    storage_account_secret : str
        The access key for the Azure Storage account.
    container_name : str
        The name of the container within the Azure Storage account.
    file_path : str
        The path to the specific blob within the container.

    Returns:
    --------
    BlobClient
        A `BlobClient` object for the specified blob, allowing for operations such as reading and writing data.
    """

    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net",
        credential=storage_account_secret
    )
    container_client = blob_service_client.get_container_client(container_name)
    return container_client.get_blob_client(file_path)


def read_json(
        storage_account_name: str, 
        storage_account_secret: str, 
        container_name: str, 
        file_path: str
    ) -> str:
    """
    Read a JSON file from Azure Data Lake Storage (ADLS) and return its content as a string.

    This function retrieves a JSON file from the specified Azure Blob Storage container and reads its
    content, returning it as a UTF-8 encoded string.

    Parameters:
    -----------
    storage_account_name : str
        The name of the Azure Storage account.
    storage_account_secret : str
        The access key for the Azure Storage account.
    container_name : str
        The name of the container within the Azure Storage account.
    file_path : str
        The path to the specific JSON file within the container.

    Returns:
    --------
    str
        The content of the JSON file as a UTF-8 encoded string.
    """

    blob_client = get_adls_blob_client(
        storage_account_name = storage_account_name, 
        storage_account_secret = storage_account_secret, 
        container_name = container_name, 
        file_path = file_path,
    )
    downloaded_blob = blob_client.download_blob()
    
    return downloaded_blob.readall().decode('utf-8')


def write_json(
        storage_account_name: str, 
        storage_account_secret: str, 
        container_name: str, 
        file_path: str, 
        data: Dict[str, Any]
    ) -> None:
    """
    Write a JSON object to a file in Azure Data Lake Storage (ADLS).

    This function serializes a Python dictionary to a JSON formatted string and uploads it to the specified
    blob in the Azure Blob Storage container. If the blob already exists, it will be overwritten.

    Parameters:
    -----------
    storage_account_name : str
        The name of the Azure Storage account.
    storage_account_secret : str
        The access key for the Azure Storage account.
    container_name : str
        The name of the container within the Azure Storage account.
    file_path : str
        The path to the specific blob within the container where the JSON data will be uploaded.
    data : Dict[str, Any]
        The Python dictionary to be serialized into JSON and uploaded. It can contain any JSON-serializable data.

    Returns:
    --------
    None
        This function does not return a value. It performs an upload operation to Azure Blob Storage.

    """

    blob_client = get_adls_blob_client(
        storage_account_name = storage_account_name, 
        storage_account_secret = storage_account_secret, 
        container_name = container_name, 
        file_path = file_path,
    )
    json_content = json.dumps(data)
    
    return blob_client.upload_blob(json_content, overwrite=True)


def read_parquet_pandas_df(
        storage_account_name: str, 
        storage_account_secret: str, 
        container_name: str, 
        file_path: str
    ) -> pd.DataFrame:
    """
    Read a Parquet file from Azure Data Lake Storage (ADLS) and return its content as a Pandas DataFrame.

    This function downloads a Parquet file from the specified Azure Blob Storage container and reads it into
    a Pandas DataFrame. The Parquet file is expected to be in binary format.

    Parameters:
    -----------
    storage_account_name : str
        The name of the Azure Storage account.
    storage_account_secret : str
        The access key for the Azure Storage account.
    container_name : str
        The name of the container within the Azure Storage account.
    file_path : str
        The path to the specific Parquet file within the container.

    Returns:
    --------
    pd.DataFrame
        The content of the Parquet file loaded into a Pandas DataFrame.
    """
    
    blob_client = get_adls_blob_client(
        storage_account_name=storage_account_name,
        storage_account_secret=storage_account_secret,
        container_name=container_name,
        file_path=file_path
    )
    download_stream = blob_client.download_blob()
    file_content = download_stream.readall()
    
    return pd.read_parquet(BytesIO(file_content))


def write_parquet_pandas_df(
        data: pd.DataFrame, 
        storage_account_name: str, 
        storage_account_secret: str, 
        container_name: str, 
        file_path: str
    ) -> None:
    """
    Write a Pandas DataFrame to a Parquet file in Azure Data Lake Storage (ADLS).

    This function serializes a Pandas DataFrame to a Parquet formatted file and uploads it to the specified
    blob in the Azure Blob Storage container. If the blob already exists, it will be overwritten.

    Parameters:
    -----------
    data : pd.DataFrame
        The Pandas DataFrame to be serialized into a Parquet file and uploaded.
    storage_account_name : str
        The name of the Azure Storage account.
    storage_account_secret : str
        The access key for the Azure Storage account.
    container_name : str
        The name of the container within the Azure Storage account.
    file_path : str
        The path to the specific blob within the container where the Parquet file will be uploaded.

    Returns:
    --------
    None
        This function does not return a value. It performs an upload operation to Azure Blob Storage.
    """

    output_buffer = BytesIO()
    data.to_parquet(output_buffer, index=False)
    output_buffer.seek(0)
    blob_client = get_adls_blob_client(
        storage_account_name = storage_account_name, 
        storage_account_secret = storage_account_secret, 
        container_name = container_name, 
        file_path = file_path,
    )
    
    return blob_client.upload_blob(output_buffer.getvalue(), overwrite=True)

def read_csv_pandas_df(
        storage_account_name: str, 
        storage_account_secret: str, 
        container_name: str, 
        file_path: str
    ) -> pd.DataFrame:
    """
    Read a CSV file from Azure Data Lake Storage (ADLS) and return its content as a Pandas DataFrame.

    This function downloads a CSV file from the specified Azure Blob Storage container and reads it into
    a Pandas DataFrame.

    Parameters:
    -----------
    storage_account_name : str
        The name of the Azure Storage account.
    storage_account_secret : str
        The access key for the Azure Storage account.
    container_name : str
        The name of the container within the Azure Storage account.
    file_path : str
        The path to the specific CSV file within the container.

    Returns:
    --------
    pd.DataFrame
        The content of the CSV file loaded into a Pandas DataFrame.
    """
    
    blob_client = get_adls_blob_client(
        storage_account_name=storage_account_name,
        storage_account_secret=storage_account_secret,
        container_name=container_name,
        file_path=file_path
    )
    download_stream = blob_client.download_blob()
    file_content = download_stream.readall()
    
    return pd.read_csv(BytesIO(file_content))

def write_csv_pandas_df(
        data: pd.DataFrame, 
        storage_account_name: str, 
        storage_account_secret: str, 
        container_name: str, 
        file_path: str
    ) -> None:
    """
    Write a Pandas DataFrame to a CSV file in Azure Data Lake Storage (ADLS).

    This function serializes a Pandas DataFrame to a CSV formatted file and uploads it to the specified
    blob in the Azure Blob Storage container. If the blob already exists, it will be overwritten.

    Parameters:
    -----------
    data : pd.DataFrame
        The Pandas DataFrame to be serialized into a CSV file and uploaded.
    storage_account_name : str
        The name of the Azure Storage account.
    storage_account_secret : str
        The access key for the Azure Storage account.
    container_name : str
        The name of the container within the Azure Storage account.
    file_path : str
        The path to the specific blob within the container where the CSV file will be uploaded.

    Returns:
    --------
    None
        This function does not return a value. It performs an upload operation to Azure Blob Storage.
    """
    
    # DataFrame in einen String-Buffer schreiben
    output_buffer = StringIO()
    data.to_csv(output_buffer, index=False)
    output_buffer.seek(0)
    
    # Blob Client erzeugen
    blob_client = get_adls_blob_client(
        storage_account_name=storage_account_name,
        storage_account_secret=storage_account_secret,
        container_name=container_name,
        file_path=file_path
    )
    
    # Datei hochladen
    blob_client.upload_blob(output_buffer.getvalue(), overwrite=True)
