import subprocess

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from radon.complexity import cc_visit

load_dotenv()

DIRECTORY = os.environ['PROJECT_DIRECTORY_TO_ANALYZE']
RECIPIENT = os.environ['RECIPIENT']

REPORT_PATH = '/tmp/project_analysis_report.txt'
PROJECT_ARCHIVE = '/tmp/project_archive.tar.gz'

default_args = {
    'start_date': datetime(2024, 6, 6),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}


def analyze_project(directory, **kwargs):
    total_files = 0
    total_lines = 0
    total_complexity = 0
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                total_files += 1
                with open(os.path.join(root, file), 'r') as f:
                    lines = f.readlines()
                    total_lines += len(lines)
                    total_complexity += sum([block.complexity for block in cc_visit(''.join(lines))])

    analysis_data = {
        'total_files': total_files,
        'total_lines': total_lines,
        'total_complexity': total_complexity
    }

    kwargs['ti'].xcom_push(key='analysis_data', value=analysis_data)


def run_pylint(**kwargs):
    result = subprocess.run(['pylint', DIRECTORY], capture_output=True, text=True)
    pylint_report = result.stdout

    kwargs['ti'].xcom_push(key='pylint_report', value=pylint_report)


def create_report(**kwargs):
    analysis_data = kwargs['ti'].xcom_pull(key='analysis_data', task_ids='analyze_project')
    pylint_report = kwargs['ti'].xcom_pull(key='pylint_report', task_ids='check_code_quality')

    with open(REPORT_PATH, 'w') as report_file:
        report_file.write(f"Total number of files: {analysis_data['total_files']}\n")
        report_file.write(f"Total lines of code: {analysis_data['total_lines']}\n")
        report_file.write(f"Total code complexity: {analysis_data['total_complexity']}\n")
        report_file.write("\n\nPylint Report:\n")
        report_file.write(pylint_report)


with DAG(
        'task_3',
        default_args=default_args,
        description='A pipeline for project analysis including code complexity, file metrics and style checks',
        schedule_interval=timedelta(days=1),
) as dag:
    analyze_task = PythonOperator(
        task_id='analyze_project',
        python_callable=analyze_project,
        op_args=[DIRECTORY],
        provide_context=True,
    )

    check_code_quality = PythonOperator(
        task_id='check_code_quality',
        python_callable=run_pylint,
        provide_context=True,
    )

    create_report_task = PythonOperator(
        task_id='create_report',
        python_callable=create_report,
        provide_context=True,
    )

    create_archive = BashOperator(
        task_id='create_project_archive',
        bash_command=f'tar -czf {PROJECT_ARCHIVE} -C {DIRECTORY} .',
    )

    send_email = EmailOperator(
        task_id='send_email',
        to=RECIPIENT,
        subject='Daily Project Analysis Report',
        html_content="""<h3>Daily Project Analysis Report</h3>
                        <p>Please find the attached analysis report for the project and project archive.</p>""",
        files=[REPORT_PATH, PROJECT_ARCHIVE],
    )

    analyze_task >> check_code_quality >> create_report_task >> create_archive >> send_email
