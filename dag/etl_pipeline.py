import datetime
import os

from omegaconf import OmegaConf
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator


dag_script_path = os.path.dirname(os.path.abspath(__file__))
config_dir_path = os.path.join(dag_script_path, "config")

# read variables config
config = OmegaConf.load(os.path.join(config_dir_path, "variables.yaml"))

# region DAG

# default dag arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 1, 1),
    "email": config.email,
    "retries": 0
}

dag = DAG(
    "nyc_taxi_etl",
    default_args=default_args,
    description="NYC taxi etl  DAG",
    catchup=False,
    schedule_interval="0 9 * * *",  # everyday at 09:00 UTC
    tags=["NYC-taxi", "etl"],
    default_view="graph",
    max_active_runs=1,
)

# generate dag documentation
dag.doc_md = __doc__

# endregion

etl_start = DummyOperator(task_id="etl_start", dag=dag)
completed = DummyOperator(task_id="completed", dag=dag)

green_taxi = BashOperator(
    task_id='green_taxi',
    bash_command=config.bash_command_green_taxi,
    dag=dag
)
yellow_taxi = BashOperator(
    task_id='yellow_taxi',
    bash_command=config.bash_command_yellow_taxi,
    dag=dag
)

etl_start >> [green_taxi, yellow_taxi] >> completed
