from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG tasks
default_args = {
    'owner': 'data_architecture_lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_hourly_etl',
    default_args=default_args,
    description='An hourly batch ETL pipeline demonstrating OLTP to OLAP Star Schema dimensional loading.',
    schedule_interval='@hourly', # Runs every hour
    start_date=datetime(2026, 6, 20),
    catchup=False, # Set to True to backfill historical days/hours automatically
    tags=['educational', 'etl', 'star-schema'],
) as dag:

    # Task: Run the main ETL script
    # It passes the start and end of the hourly execution window dynamically using Airflow Jinja templates
    run_etl = BashOperator(
        task_id='run_ecommerce_etl_pipeline',
        bash_command=(
            'python /opt/airflow/scripts/etl_pipeline.py '
            '--host mysql '
            '--start_time "{{ data_interval_start.strftime(\'%Y-%m-%d %H:%M:%S\') }}" '
            '--end_time "{{ data_interval_end.strftime(\'%Y-%m-%d %H:%M:%S\') }}"'
        )
    )

    run_etl
