import os
import requests
from dotenv import load_dotenv


#Reading parameters from .env file
load_dotenv()
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
TOKEN = os.getenv("DATABRICKS_TOKEN")

headers = {
    "Authorization": f"Bearer {TOKEN}"
}

def get_error_message_from_run_output(run_id):
    """
    Calls the Databricks API to fetch error output for a specific job run ID.
    """
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        error_message = data.get("error", "No error message found.")
        #print(f"\nError Message from Job Run ID {run_id}:\n{error_message}")
        notebook_path = data.get("metadata").get('tasks')[0]['notebook_task']['notebook_path']
        return {"error_message":error_message,"Path":notebook_path}
    else:
        print(f"Failed to retrieve output. Status Code: {response.status_code}")
        print(response.text)
        return None
