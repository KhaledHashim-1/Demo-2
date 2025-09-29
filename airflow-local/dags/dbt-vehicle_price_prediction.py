from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="dbt-vehicle_price_prediction",
    start_date=datetime(2025, 9, 21),
    schedule_interval=None,  
    catchup=False,
    default_args=default_args,
) as dag:

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/dbt/my_dbt_project && "
            "dbt run --select staging --profiles-dir /opt/airflow/dbt"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/airflow/dbt/my_dbt_project && "
            "dbt run --select marts --profiles-dir /opt/airflow/dbt"
        ),
    )

    dbt_run_reporting = BashOperator(
        task_id="dbt_run_reporting",
        bash_command=(
            "cd /opt/airflow/dbt/my_dbt_project && "
            "dbt run --select reporting --profiles-dir /opt/airflow/dbt"
        ),
    )

    
    dbt_run_staging >> dbt_run_marts >> dbt_run_reporting
