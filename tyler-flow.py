from prefect import flow, task
from prefect_databricks import DatabricksCredentials
from prefect_databricks.flows import jobs_runs_submit_by_id_and_wait_for_completion
from prefect_snowflake import  SnowflakeConnector
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

@flow()
def trigger_dbx_ingestion():
    creds = DatabricksCredentials.load("tyler-databricks-creds")
    run = jobs_runs_submit_by_id_and_wait_for_completion(       
        databricks_credentials=creds,
        job_id=874003545683509
    )
    return run

@task
def create_edl_tables():
    with SnowflakeConnector.load("tyler-snowflake-connector") as connector:
        connector.execute("USE ROLE qa_service_ent_etl;")
        connector.execute("CREATE SCHEMA IF NOT EXISTS tyler_EDL")
        connector.execute(
            """
            create or replace ICEBERG TABLE WBMIQA_ORCHPOC_DB.tyler_EDL.YELLOW_TAXI_TRIPS
                CATALOG = 'ADLS_WBCDAQPSEUDODLSA_CATALOG_DELTA'
                EXTERNAL_VOLUME = 'ADLS_WBCDAQPSEUDODLSA_ICEBERG' 
                BASE_LOCATION = 'tyler/yellow_taxi_trips/';
            """
        )
        connector.execute(
            """
            create or replace ICEBERG TABLE WBMIQA_ORCHPOC_DB.tyler_EDL.TAXI_ZONE_LOOKUP
                CATALOG = 'ADLS_WBCDAQPSEUDODLSA_CATALOG_DELTA'
                EXTERNAL_VOLUME = 'ADLS_WBCDAQPSEUDODLSA_ICEBERG' 
                BASE_LOCATION = 'tyler/taxi_zone_lookup/';
            """
        )

@task
def run_dbt():
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir="snowflake",
            profiles_dir="snowflake"
        )
    ).invoke(["deps"])
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir="snowflake",
            profiles_dir="snowflake"
        )
    ).invoke(["build", "--vars", "{env: TYLER}"])

@task()
def export_file(pickup_year: int, pickup_month: int):
    with SnowflakeConnector.load("tyler-snowflake-connector") as connector:
        connector.execute("USE ROLE qa_service_ent_etl;")
        connector.execute("USE DATABASE WBMIQA_ORCHPOC_DB;")
        connector.execute("USE SCHEMA tyler_EDL;")
        padded_month = f"{pickup_month:02d}"
        connector.execute(
            """
            CREATE STAGE IF NOT EXISTS TYLER_MRT.EXPORT
            URL = 'azure://wbcdaqpseudodlsa.blob.core.windows.net/outbound/tyler/'
            STORAGE_INTEGRATION = AZ_QA_PSEUDO_STORAGE_INTEGRATION
            DIRECTORY = (
                ENABLE = FALSE
                AUTO_REFRESH = FALSE
            );
            """            
        )
        connector.execute(
            f"""
            COPY INTO @TYLER_MRT.EXPORT/EXPORT/AGG_MONTHLY_ZONE_SUMMARY_{pickup_year}_{padded_month}
            FROM (
                SELECT *
                FROM tyler_MRT.AGG_MONTHLY_ZONE_SUMMARY
                WHERE PICKUP_YEAR = {pickup_year}
                AND PICKUP_MONTH = {pickup_month}
            )
            FILE_FORMAT = (TYPE = PARQUET)
            OVERWRITE = TRUE;
            """
        )

@flow()
def trigger_dbx_export():
    creds = DatabricksCredentials.load("tyler-databricks-creds")
    run = jobs_runs_submit_by_id_and_wait_for_completion(       
        databricks_credentials=creds,
        job_id=646082920407301
    )
    return run

@flow(name="Tyler - Main Flow", log_prints=True)
def main_flow():
    trigger_dbx_ingestion()
    create_edl_tables()
    run_dbt()
    export_file(pickup_year=2025, pickup_month=1)
    trigger_dbx_export()

if __name__ == "__main__":
    #main_flow()
    #main_flow.serve(name="Tyler - Main Flow", cron="0 0 1 * *")
    flow.from_source(
        source="https://github.com/tylerfaulkner/orchestration-poc.git",
        entrypoint="tyler-flow.py:main_flow"
    ).deploy(
        name='Tyler - Deployed Flow',
        work_pool_name='tyler-managed-pool',
        job_variables={"image": "prefecthq/prefect:3-latest", "pip_packages": ["pandas", "azure-identity", "azure-keyvault-secrets", "dbt-core", "prefect[databricks]", "prefect[snowflake]", "prefect[dbt]", "prefect[snowflake]", "dbt-snowflake", "prefect-dbt[snowflake]"]},
    )