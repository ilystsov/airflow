import random
import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'kosarevsky',
    'start_date': dt.datetime(2024, 5, 28),
    'retries': 2,
    'retry_delay': dt.timedelta(seconds=10),
}


def random_dice():
    val = random.randint(1, 6)
    if val % 2 != 0:
        raise ValueError(f'Odd {val}')


with DAG(dag_id='01_dag_1',
         schedule_interval='@hourly',
         default_args=default_args) as dag:

    dice = PythonOperator(
        task_id='random_dice',
        python_callable=random_dice,
        dag=dag,
    )
