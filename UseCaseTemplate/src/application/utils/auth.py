from azure.keyvault.secrets import SecretClient
from typing import Optional, Any # used for type annotation

def get_keyvault_secrets(
        keyvault_name: str,
        secret_name: str,
        run_environment: str,
        credential: Optional[Any] = None,
        mssparkutils: Optional[Any] = None,
        keyvault_linkedservice_name: Optional[str] = None,
    ) -> str:
    """
    Retrieve a secret from Azure KeyVault based on the execution environment.

    This function allows for the retrieval of secrets from Azure KeyVault in different execution environments.

    Parameters:
    -----------
    keyvault_name : str
        The name of the Azure KeyVault from which the secret will be retrieved.
    secret_name : str
        The name of the secret to retrieve from the KeyVault.
    run_environment : str
        The environment in which the function is being run. 
        - Use "vs_code" for local development in Visual Studio Code.
        - Use "synapse" for execution within Azure Synapse.
    credential : Optional[Any]
        The credentials used to authenticate with Azure KeyVault when running locally ("vs_code").
        This parameter is not required when running in the Synapse environment.
    mssparkutils : Optional[Any]
        The Azure Synapse utility module, used to interact with KeyVault secrets in the Synapse environment.
        Required only when `run_environment` is "synapse".
    keyvault_linkedservice_name : Optional[str]
        The name of the linked service in Azure Synapse that is associated with the Azure KeyVault.
        Required only when `run_environment` is "synapse".

    Returns:
    --------
    str
        The value of the secret retrieved from the Azure KeyVault.
    """

    if run_environment == "vs_code":
        if not keyvault_name or not secret_name:
            raise ValueError(
                "Missing parameters for vs_code: 'keyvault_name' and 'secret_name' must " "be set. "
            )
        keyvault_url = f"https://{keyvault_name}.vault.azure.net"
        keyvault_client = SecretClient(vault_url=keyvault_url, credential=credential)
        return keyvault_client.get_secret(secret_name).value

    elif run_environment == "synapse":
        if not keyvault_name or not secret_name or not keyvault_linkedservice_name:
            raise ValueError(
                "Missing parameters for RunEnvironment.SYNAPSE: 'keyvault_name', 'secret_name' and "
                "'keyvault_linkedservice_name' must be set. "
            )

    return mssparkutils.credentials.getSecret(
                keyvault_name,
                secret_name,
                keyvault_linkedservice_name,
            )