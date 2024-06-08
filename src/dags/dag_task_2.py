from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime


with DAG('task_2', start_date=datetime(2024, 6, 6), schedule_interval='@daily') as dag:
    start = DummyOperator(task_id='start', dag=dag)
    stop_task = DummyOperator(task_id='stop_task', dag=dag)

    t1 = DummyOperator(task_id='t1', dag=dag)

    t2_1 = DummyOperator(task_id='t2_1', dag=dag)
    t2_2 = DummyOperator(task_id='t2_2', dag=dag)
    t2_3 = DummyOperator(task_id='t2_3', dag=dag)

    t3_1 = DummyOperator(task_id='t3_1', dag=dag)
    t3_2 = DummyOperator(task_id='t3_2', dag=dag)
    t3_3 = DummyOperator(task_id='t3_3', dag=dag)

    t4 = DummyOperator(task_id='t4', dag=dag)

    end = DummyOperator(task_id='end', dag=dag)

    start >> [stop_task, t1]
    t1 >> t2_1 >> t3_1 >> t4
    t1 >> t2_2 >> t3_2 >> t4
    t1 >> t2_3 >> t3_3 >> t4
    t4 >> end

