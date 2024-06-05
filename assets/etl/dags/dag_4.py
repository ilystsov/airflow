from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(dag_id='04_dag_with_many_tasks_01', schedule_interval='@daily', default_args=default_args) as dag:
    t1 = DummyOperator(task_id='task_1', dag=dag)
    t2 = DummyOperator(task_id='task_2', dag=dag)
    t3 = DummyOperator(task_id='task_3', dag=dag)
    t4 = DummyOperator(task_id='task_4', dag=dag)
    t5 = DummyOperator(task_id='task_5', dag=dag)
    t6 = DummyOperator(task_id='task_6', dag=dag)
    t7 = DummyOperator(task_id='task_7', dag=dag)

    [t1, t2, t3] >> t5 >> t7
    [t2, t4] >> t6 >> t7
    t4 >> t7
    # t3 >> t6
