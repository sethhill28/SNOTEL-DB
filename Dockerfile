FROM apache/airflow:2.5.1
USER airflow
COPY requirements.txt .
RUN pip install -r requirements.txt
