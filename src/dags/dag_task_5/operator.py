from typing import Any
from airflow.models.baseoperator import BaseOperator
from .hook import WeatherHook


class WeatherOperator(BaseOperator):
    def __init__(
            self,
            location: str,
            conn_id: str = 'weather_conn_id',
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.location = location

    def execute(self, context: Any):
        api = WeatherHook(self.conn_id)
        weather_data = api.get_weather(context['execution_date'].date(), self.location)
        return weather_data
