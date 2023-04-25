from airflow import DAG 
from datetime import date, datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


import utilities as util
import config

default_args = {
    'owner': 'airflow', 
    'start_date': datetime(2023, 3, 28), 
    'retries': 1, 
    'retry_delay': timedelta(seconds=5), 
    'schedule_interval': None, 
    'catchup': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [config.email],
}

schema_fields=[
            {'name': 'element_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'element_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'units', 'type': 'STRING', 'mode': 'NULLABLE'}
]

src_file_path = '/opt/airflow/Data/data-dictionary.csv'
dst_file_path = 'data-dictionary.csv'

dataset_name = 'snotel_dw'
stg_table_name = 'data_dictionary_stg'
prod_table_name = 'data_dictionary_prod'
source_objects = ['data-dictionary.csv']

# GCP hook
prod_hook = BigQueryHook(gcp_conn_id='gcp_conn', location = 'us-west3')
prod_conn = prod_hook.get_conn()
prod_cursor = prod_conn.cursor()

# record count checks
prod_record_count_query = f'Select count(*) from {dataset_name}.{prod_table_name}'
prod_cursor.execute(prod_record_count_query)
prod_record_count = prod_cursor.fetchall()

def success_branch():
    if prod_record_count[0][0] > 0:
        return ['delete_gcs_file', 'delete_local_file']
    else:
        return ['failure_email']

with DAG('LOAD_DATA_DICTIONARY', 
         default_args=default_args) as dag:
    
    t1 = PythonOperator(
        task_id = 'write_local_csv', 
        python_callable = util.create_data_dictionary,
        email_on_failure = True,
        email = 'sethhill28@gmail.com'
    )

    t2 = LocalFilesystemToGCSOperator(
        task_id = 'upload_local_csv_to_gcs', 
        src = src_file_path, 
        dst = dst_file_path,
        gcp_conn_id = 'gcp_conn', 
        bucket = 'airflow-snotel'
    )

    t3 = GCSToBigQueryOperator(
        task_id = 'upload_gcs_file_to_bigquery_stg',
        bucket = 'airflow-snotel',
        gcp_conn_id = 'gcp_conn',
        source_objects = source_objects,
        write_disposition = 'WRITE_TRUNCATE',
        destination_project_dataset_table=f"{dataset_name}.{stg_table_name}",
        schema_fields = schema_fields,
        autodetect = False, 
        skip_leading_rows = 1
    )

    t4 = BigQueryToBigQueryOperator(
        task_id = 'update_prod_table',
        gcp_conn_id = 'gcp_conn',
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        source_project_dataset_tables = [f"{dataset_name}.{stg_table_name}"],
        destination_project_dataset_table = f"{dataset_name}.{prod_table_name}",
        location = 'us-west3'
    )

    t5 = BranchPythonOperator(
        task_id = 'success_or_failure_branch',
        python_callable = success_branch
    )

    t6 = BashOperator(
        task_id = 'delete_local_file',
        bash_command = f'rm {src_file_path}'
    )

    t7 = GCSDeleteObjectsOperator(
        task_id = 'delete_gcs_file',
        bucket_name = 'airflow-snotel',
        gcp_conn_id = 'gcp_conn', 
        objects = [dst_file_path]
    )

    t8 =  EmailOperator(
        task_id = 'failure_email',
        to='sethhill28@gmail.com', 
        subject= f'{dag.dag_id} failed to load data', 
        html_content=f""" <h1> {dag.dag_id} failed to load data </h1> """
    )


t1 >> t2 >> t3 >> t4 >> t5 >> t8
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7


