import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from tasks.data_proccess import read_exec_data_stg
from tasks.sale_data_stg import stg_shema_generate
from tasks.sale_data_stg0 import stg0_shema_generate
from tasks.vault import vault_schema_generate
from tasks.mart import mart_schema_generate

dag_args = {
    'owner': 'airflow',
    'start_date':datetime(2024, 3, 17),
    'catchup':False,
}

dag = DAG(
    dag_id = 'csv_to_postgres', 
    default_args=dag_args,
    #schedule_interval="*/30 * * * *",  # Runs every 30 minutes
)

task_read_exec_data_stg = PythonOperator(
    task_id = "read_exec_data_stg_schema",
    python_callable = read_exec_data_stg,
    provide_context=True,
    dag=dag
)

task_stg0_schema = PythonOperator(
    task_id="write_stg0_schema",
    python_callable=stg0_shema_generate,
    provide_context=True,
    dag=dag
)

task_vault_schema = PythonOperator(
    task_id="write_vault_schema",
    python_callable=vault_schema_generate,
    provide_context=True,
    dag=dag
)

task_mart_schema = PythonOperator(
    task_id="write_mart_schema",
    python_callable=mart_schema_generate,
    provide_context=True,
    dag=dag
)

task_read_exec_data_stg >> task_stg0_schema >> task_vault_schema >> task_mart_schema
