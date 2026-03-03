import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

#  Set IST timezone properly
local_tz = pendulum.timezone("Asia/Kolkata")

#  Full path to your script
SCRIPT_PATH = r"C:\Users\User\sunrise-data-pipeline\incremental.py"

default_args = {
    "owner": "shivani",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sunrise_incremental_daily",
    default_args=default_args,
    description="Runs incremental.py daily at 9 PM IST",
    schedule="0 21 * * *",   # 9:00 PM IST
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    catchup=False,
    tags=["sunrise", "incremental"],
) as dag:

    run_incremental = BashOperator(
        task_id="run_incremental_script",
        bash_command=f'python "{SCRIPT_PATH}"',
    )

    run_incremental