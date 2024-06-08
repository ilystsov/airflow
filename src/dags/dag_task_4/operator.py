from typing import Any

from airflow.models.baseoperator import BaseOperator
import matplotlib.pyplot as plt


class PlotGenerationOperator(BaseOperator):
    def __init__(
            self,
            output_file_name: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.output_file_name = output_file_name

    def execute(self, context: Any):
        data = context['ti'].xcom_pull(task_ids='generate_data')
        plt.figure(figsize=(10, 6))
        plt.plot(data['x'], data['y'], marker='o')
        plt.title('Generated Graph')
        plt.xlabel('X axis')
        plt.ylabel('Y axis')
        plt.grid(True)
        plt.savefig(self.output_file_name)