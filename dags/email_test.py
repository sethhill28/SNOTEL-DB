from airflow import DAG 
from datetime import date, datetime, timedelta
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow', 
    'start_date': datetime(2023, 3, 22), 
    'retries': 1, 
    'retry_delay': timedelta(seconds=5), 
    'schedule_interval': None
}


with DAG('SNOTEL_DAG', 
         default_args=default_args, 
         catchup=False) as dag:   
    t1 = EmailOperator(task_id='send_email', 
                    to='sethhill28@gmail.com', 
                    subject='Daily report generated', 
                    html_content=""" <h1>Congratulations! Your SNOTEL reports are ready.</h1> """)