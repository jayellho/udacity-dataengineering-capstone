from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from udacity_capstone_project import config
from udacity_capstone_project.scripts import etl_resale_prices

dag = DAG(
    'etl_resale_prices',
    description = 'Load HDB resale prices data into data warehouse',
    schedule_interval = '@monthly',
    start_date = config.START["START_DATE"],
)


resale_prices_api_to_csv = PythonOperator(
    task_id = 'resale_prices_api_to_csv',
    dag = dag,
    python_callable = etl_resale_prices.api_to_csv,
)

resale_prices_csv_to_dwh = PythonOperator(
    task_id = 'resale_prices_csv_to_dwh',
    dag = dag,
    python_callable = etl_resale_prices.csv_to_dwh,
)

resale_prices_check_rows_inserted = PythonOperator(
    task_id = 'resale_prices_check_rows_inserted',
    dag = dag,
    python_callable = etl_resale_prices.check_rows_inserted,
)

resale_prices_api_to_csv >> resale_prices_csv_to_dwh >> resale_prices_check_rows_inserted
