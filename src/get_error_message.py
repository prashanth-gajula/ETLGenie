import os
import requests
from dotenv import load_dotenv
from DatabricksJobManager import (
    list_databricks_jobs,
    get_latest_job_run_id,
    get_task_run_ids
)

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
        print(f"\nError Message from Job Run ID {run_id}:\n{error_message}")
        return error_message
    else:
        print(f"Failed to retrieve output. Status Code: {response.status_code}")
        print(response.text)
        return None

if __name__ == "__main__":
    # Step 1: Get Job IDs
    job_id = list_databricks_jobs()
    if not job_id:
        exit("No jobs available.")

    # Select the first job (or filter by name as needed)
    #job_id = jobs[0]["job_id"]
    #print(f"Selected Job ID: {job_id}")

    # Step 2: Get latest run and task info
    run_info = get_latest_job_run_id(job_id)
    if not run_info:
        exit("No run info found.")

    job_run_id = run_info["job_run_id"]
    print(f"Job Run ID: {job_run_id}")

    # Step 3: Get Task Run Id
    Task_run_id = get_task_run_ids(job_run_id)

    # Step 4: Get and print error message for the Job Run ID
    get_error_message_from_run_output(Task_run_id)