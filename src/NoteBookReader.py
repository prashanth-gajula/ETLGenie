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
    """
    Fetches and decodes the source code of a Databricks notebook.
    :param notebook_path: Full workspace path to the notebook (e.g., /Workspace/Users/.../NotebookName)
    :return: The decoded notebook source code as a string, or None if failed.
    """
   
    if not DATABRICKS_HOST or not TOKEN:
        raise ValueError("Missing DATABRICKS_HOST or DATABRICKS_TOKEN in .env")

    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }

    params = {
        "path": notebook_path,
        "format": "SOURCE"
    }

    url = f"{DATABRICKS_HOST}/api/2.0/workspace/export"

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        try:
            base64_content = response.json()["content"]
            decoded = base64.b64decode(base64_content).decode("utf-8")
            return decoded
        except Exception as e:
            print("Error decoding content:", e)
            return None
    else:
        print(f"Failed to fetch notebook. Status code: {response.status_code}")
        print(response.text)
        return None

# Example usage
if __name__ == "__main__":
    #notebook_path = "/Workspace/Users/kumargajula782@gmail.com/AppleAnalysis/Transform"
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
    #print(f"Job Run ID: {job_run_id}")

    # Step 3: Get Task Run Id
    Task_run_id = get_task_run_ids(job_run_id)

    # Step 4: Get and print error message for the Job Run ID
    result = get_error_message_from_run_output(Task_run_id)
    #print(result.get("Path"))
    print(fetch_notebook_source(result.get("Path")))
    