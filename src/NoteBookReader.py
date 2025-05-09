import requests
import base64
import os
from dotenv import load_dotenv

from DatabricksJobManager import (
    list_databricks_jobs,
    get_latest_job_run_id,
    get_task_run_ids
)
from get_error_message import get_error_message_from_run_output

load_dotenv()
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
TOKEN = os.getenv("DATABRICKS_TOKEN")


def fetch_notebook_source(notebook_path):
    if not DATABRICKS_HOST or not TOKEN:
        raise ValueError("Missing DATABRICKS_HOST or DATABRICKS_TOKEN in .env")

    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {"path": notebook_path, "format": "SOURCE"}
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/export"

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        try:
            base64_content = response.json()["content"]
            return base64.b64decode(base64_content).decode("utf-8")
        except Exception as e:
            print("Error decoding content:", e)
            return None
    else:
        print(f"Failed to fetch notebook. Status code: {response.status_code}")
        print(response.text)
        return None



