from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from .operator import CurrencyScoopOperator

with DAG(
        dag_id='07_exchange_rate_usd_rub_dag_simple',
        start_date=datetime(2024, 5, 1),
        schedule_interval='@daily',
        max_active_runs=3,
) as dag:
    create_table = PostgresOperator(
        task_id='create_table_task',
        sql='sql/create_table.sql',
        postgres_conn_id='postgres_tink_seminar',
    )

    get_rate = CurrencyScoopOperator(
        task_id='get_rate',
        base_currency='USD',
        currency='RUB',
        conn_id='cur_scoop',
        dag=dag,
        do_xcom_push=True,
    )

    insert_rate = PostgresOperator(
        task_id='insert_rate',
        postgres_conn_id='postgres_tink_seminar',
        sql='sql/insert_rate.sql',
        params={
            'base_currency': 'USD',
            'currency': 'RUB',
        }
    )

    create_table >> get_rate >> insert_rate
