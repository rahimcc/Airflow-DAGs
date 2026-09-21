import os
from airflow.models import DagBag


DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")