from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from .operator import WeatherOperator

default_args = {
    'start_date': datetime(2024, 6, 6),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}


def format_weather_data(**context):
    weather_data = context['ti'].xcom_pull(task_ids='get_weather')
    forecast = weather_data['forecast']['forecastday'][0]['day']
    formatted_data = {
        'location': weather_data['location']['name'],
        'date': weather_data['forecast']['forecastday'][0]['date'],
        'avg_temp_c': forecast['avgtemp_c'],
        'condition': forecast['condition']['text']
    }
    context['ti'].xcom_push(key='formatted_weather_data', value=formatted_data)


with DAG(
        dag_id='task_5',
        schedule_interval=timedelta(days=1),
        default_args=default_args,
) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        sql='sql/create_table.sql',
        postgres_conn_id='weather_db_conn_id',
    )

    get_weather = WeatherOperator(
        task_id='get_weather',
        location='London',
        dag=dag,
        do_xcom_push=True,
    )

    format_data = PythonOperator(
        task_id='format_weather_data',
        python_callable=format_weather_data,
        provide_context=True,
    )

    insert_weather = PostgresOperator(
        task_id='insert_weather',
        postgres_conn_id='weather_db_conn_id',
        sql='sql/insert_weather.sql',
    )

    create_table >> get_weather >> format_data >> insert_weather
