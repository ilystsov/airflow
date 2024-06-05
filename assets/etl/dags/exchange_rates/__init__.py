from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from .operator import CurrencyScoopOperator


with DAG(
        dag_id='08_exchange_rate_usd_rub_dag',
        start_date=datetime(2022, 4, 23),
        schedule_interval='@daily',
        max_active_runs=1,
) as dag:

    create_table = PostgresOperator(
        task_id='create_table_task',
        sql='sql/create_table.sql',
        postgres_conn_id='postgres_tink_seminar',
    )

    tasks = []

    for base, currency in [
        ('USD', 'BYN'),
        ('USD', 'RUB'),
        ('USD', 'EUR'),
        ('BYN', 'RUB'),
        ('RUB', 'BYN'),
        ('EUR', 'BYN'),
        ('EUR', 'RUB'),
    ]:
        get_rate_task = CurrencyScoopOperator(
            task_id=f'get_rate_{ base }_{ currency }',
            base_currency=base,
            currency=currency,
            conn_id='cur_scoop',
            dag=dag,
            do_xcom_push=True,
        )

        insert_rate = PostgresOperator(
            task_id=f'insert_rate_{ base }_{ currency }',
            postgres_conn_id='postgres_tink_seminar',
            sql='sql/insert_rate.sql',
            params={
                'base_currency': base,
                'currency': currency,
                'get_rate_task_id': f'get_rate_{ base }_{ currency }'
            }
        )

        get_rate_task >> insert_rate

        tasks.append(get_rate_task)

    create_table.set_downstream(tasks)
