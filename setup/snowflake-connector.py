from prefect_snowflake import SnowflakeCredentials, SnowflakeConnector

credentials = SnowflakeCredentials.load("tyler-snowflake-creds")

connector = SnowflakeConnector(
    credentials=credentials,
    database="WBMIQA_ORCHPOC_DB",
    schema="tyler_EDL",
    warehouse="VWH_ADHOC",
)
connector.save("tyler-snowflake-connector", overwrite=True)