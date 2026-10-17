
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.operators.bash import BashOperator


# Path inside the Astro Airflow container
DBT_PROJECT_DIR = "/usr/local/airflow/logistic"


@dag(
    dag_id="orchestrate",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["logistics", "azure", "dbt"],
)
def orchestrate():

   
    ingest_cdc = AzureDataFactoryRunPipelineOperator(
        task_id="run_adf_pipeline",
        azure_data_factory_conn_id="azure_data_factory",
        resource_group_name="logistics",
        factory_name="logistics",
        pipeline_name="logistics",
        wait_for_termination=True,
    )

    # 2. Clean previous dbt artifacts
    clean_target = BashOperator(
        task_id="clean_target",
        bash_command=f"rm -rf {DBT_PROJECT_DIR}/target",
    )

    # 3. Install dbt dependencies and check source freshness
    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            "&& dbt deps "
            "&& dbt source freshness"
        ),
    )

    build_dbt = BashOperator(
        task_id="build_dbt",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            "&& dbt build"
        ),
    )

    # Define execution order
    (
        ingest_cdc
        >>clean_target
        >> source_freshness
        >> build_dbt
    )


orchestrate()
