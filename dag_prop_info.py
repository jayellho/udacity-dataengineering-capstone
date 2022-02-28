from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from udacity_capstone_project import config
from udacity_capstone_project.scripts import etl_prop_info

dag = DAG(
    'etl_fact_prop_info',
    description = 'Load HDB property information data into data warehouse',
    schedule_interval = '@monthly',
    start_date = config.START["START_DATE"],
)


prop_info_api_to_csv = PythonOperator(
    task_id = 'prop_info_api_to_csv',
    dag = dag,
    python_callable = etl_prop_info.api_to_csv,
)

prop_info_csv_to_dwh = PythonOperator(
    task_id = 'prop_info_csv_to_dwh',
    dag = dag,
    python_callable = etl_prop_info.csv_to_dwh,
)

prop_info_check_rows_inserted = PythonOperator(
    task_id = 'prop_info_check_rows_inserted',
    dag = dag,
    python_callable = etl_prop_info.check_rows_inserted,
)


prop_info_api_to_csv >> prop_info_csv_to_dwh >> prop_info_check_rows_inserted
