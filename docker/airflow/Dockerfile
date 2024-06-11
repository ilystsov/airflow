FROM apache/airflow:2.9.1

RUN pip install poetry

COPY pyproject.toml poetry.lock /opt/airflow/

WORKDIR /opt/airflow

RUN poetry config virtualenvs.create false && poetry install --no-dev

COPY .env /opt/airflow/