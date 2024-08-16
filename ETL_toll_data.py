from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'randomOG',
    'start_date': days_ago(0),
    'email': ['randomOG@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tg7',
    dag=dag,
)
extract_data_from_csv = BashOperator (
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/cs_data.csv',
    dag=dag,
)
extract_data_from_tsv = BashOperator(
    task_id= "extract_data_from_tsv",
    bash_command='cut -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.tsv > /home/project/airflow/dags/finalassignment/ts_data.csv',
    dag=dag,
)
extract_data_from_fixed_width = BashOperator(
    task_id= "extract_data_from_fixed_width",
    bash_command='curt -c1-4 /home/project/airflow/dags/finalassignment/vehicle-data-fixed-width.txt > /home/project/airflow/dags/finalassignment/fw_data.csv',
    dag=dag,
)
consolidate_data = BashOperator(
    task_id= "consolidate_data",
    bash_command='cat /home/project/airflow/dags/finalassignment/cs_data.csv /home/project/airflow/dags/finalassignment/ts_data.csv /home/project/airflow/dags/finalassignment/fw_data.csv > /home/project/airflow/dags/finalassignment/consolidated_data.csv',
    dag=dag,
)
transform_data = BashOperator(
    task_id= "transform_data",
    bash_command='tr " [a-z]" "[A-Z]" < /home/project/airflow/dags/finalassignment/extracted data.csv \
    > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag,
)
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data