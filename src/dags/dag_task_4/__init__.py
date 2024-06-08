from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import os

from dotenv import load_dotenv

from .operator import PlotGenerationOperator

load_dotenv()

GRAPH_PATH = '/tmp/generated_graph.png'
RECIPIENT = os.environ['RECIPIENT']

default_args = {
    'start_date': datetime(2024, 6, 6),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}


def generate_data():
    return {
        'x': list(range(10)),
        'y': [i ** 2 for i in range(10)]
    }


with DAG(
        'task_4',
        default_args=default_args,
        description='A pipeline for generating graphs and sending them via email',
        schedule_interval=timedelta(days=1),
) as dag:
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
        do_xcom_push=True,
    )

    generate_plot = PlotGenerationOperator(
        task_id='generate_plot',
        output_file_name=GRAPH_PATH,
    )

    send_email = EmailOperator(
        task_id='send_email',
        to=RECIPIENT,
        subject='Daily Generated Graph',
        html_content="""<h3>Daily Generated Graph</h3>
                        <p>Please find the attached generated graph.</p>""",
        files=[GRAPH_PATH],
    )

    generate_data >> generate_plot >> send_email
