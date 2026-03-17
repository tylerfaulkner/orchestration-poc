from prefect_databricks import DatabricksCredentials
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient

identity = AzureCliCredential()
key_vault_url = "https://orchpockv.vault.azure.net/"
secret_client = SecretClient(vault_url=key_vault_url, credential=identity)

# spn_appid = secret_client.get_secret("tyler-dbx-spn-appid").value
# client_secret = secret_client.get_secret("tyler-dbx-spn-key").value

# print(spn_appid)

# credentials = DatabricksCredentials(
#     databricks_instance="adb-2969996502866095.15.azuredatabricks.net",
#     client_id=spn_appid,
#     client_secret=client_secret,
#     tenant_id="48b0431c-82f6-4ad2-a023-ac96dbf5614e"
# )

pat_token = secret_client.get_secret("tyler-pat-token").value
credentials = DatabricksCredentials(
    databricks_instance="adb-2969996502866095.15.azuredatabricks.net",
    token=pat_token
)
credentials.save("tyler-databricks-creds", overwrite=True)