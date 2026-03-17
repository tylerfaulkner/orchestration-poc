from prefect_snowflake import SnowflakeCredentials
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient

identity = AzureCliCredential()
key_vault_url = "https://orchpockv.vault.azure.net/"
secret_client = SecretClient(vault_url=key_vault_url, credential=identity)
snowflake_password = secret_client.get_secret("snowflake-poc").value
print(snowflake_password)

credentials = SnowflakeCredentials(
    account="WBMI-ENTERPRISE",  # resembles nh12345.us-east-2.snowflake
    user="QA_SERVICE_USER_POC",
    password=snowflake_password
)
credentials.save("tyler-snowflake-creds", overwrite=True)