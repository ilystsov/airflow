import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from dotenv import load_dotenv

load_dotenv()


class WeatherHook(BaseHook):
    def __init__(self, weather_conn_id: str):
        super().__init__()
        self.conn_id = weather_conn_id

    def get_weather(self, date, location: str):
        params = {
            'q': location,
            'dt': str(date),
            'key': self._get_api_key(),
        }
        url = Variable.get('weather_api_url')
        if not url:
            raise AirflowException('Missing API URL in Airflow Variables')
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _get_api_key(self):
        conn = self.get_connection(self.conn_id)
        if not conn.password:
            raise AirflowException('Missing API key (password) in connection settings')
        return conn.password
