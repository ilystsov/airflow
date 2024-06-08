from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime


with DAG('task_1', start_date=datetime(2024, 6, 6), schedule_interval='@daily') as dag:
    start = DummyOperator(task_id='start', dag=dag)
    op1 = DummyOperator(task_id='op-1', dag=dag)
    op2 = DummyOperator(task_id='op-2', dag=dag)
    some_other_task = DummyOperator(task_id='some-other-task', dag=dag)
    op3 = DummyOperator(task_id='op-3', dag=dag)
    op4 = DummyOperator(task_id='op-4', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)

    start >> op1 >> some_other_task >> op4 >> end
    start >> op2 >> some_other_task >> op3 >> end
