import requests
import os
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
TOKEN = os.getenv("DATABRICKS_TOKEN")


def list_databricks_jobs():
    """
    Fetch and print all job IDs and job names from a Databricks workspace.

    Requirements:
    - DATABRICKS_TOKEN and DATABRICKS_HOST must be set in a .env file.
    - DATABRICKS_HOST should be the base URL, e.g., https://adb-xxxx.azuredatabricks.net
    """

    # Load environment variables from .env
 
    if not DATABRICKS_HOST or not TOKEN:
        print("Missing environment variables. Please check .env file.")
        return

    # Set up request headers
    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }

    # Endpoint to list all jobs
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/list"

    # Make the request
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        jobs = response.json().get("jobs", [])

        if not jobs:
            print("No jobs found.")
        else:
            #print("Jobs found:")
            for job in jobs:
                job_id = job.get("job_id")
                job_name = job.get("settings", {}).get("name", "Unnamed Job")
                #print(f"Job ID: {job_id}, Name: {job_name}")
            return job_id
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

#generating Job RunId and Task Run Id using the job Id 

def get_latest_job_run_id(job_id):
    """
    Given a Job ID, fetch the most recent Job Run ID and associated Task Run IDs (if any).
    Requires DATABRICKS_TOKEN and DATABRICKS_HOST in the .env file.
    """


    if not DATABRICKS_HOST or not TOKEN:
        print("Missing DATABRICKS_HOST or DATABRICKS_TOKEN in environment.")
        return None

    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }

    # API to get latest run of the job
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/list?job_id={job_id}&limit=1"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        runs = response.json().get("runs", [])

        if not runs:
            print(f"No runs found for job ID {job_id}")
            return None

        latest_run = runs[0]
        job_run_id = latest_run.get("run_id")
        run_name = latest_run.get("run_name", "Unnamed Run")

        #print(f"\nLatest Run for Job ID {job_id}:")
        #print(f"Job Run ID: {job_run_id}, Run Name: {run_name}")

        # Check for tasks (only present in workflows / multi-task jobs)
        """task_run_ids = []
        if "tasks" in latest_run:
            print("Task Run IDs:")
            for task in latest_run["tasks"]:
                task_key = task.get("task_key")
                task_run_id = task.get("run_id")
                task_state = task.get("state", {}).get("result_state", "UNKNOWN")
                print(f"  Task Key: {task_key}, Task Run ID: {task_run_id}, Status: {task_state}")
                task_run_ids.append((task_key, task_run_id, task_state))
        else:
            print("This is a single-task job.")"""

        return {
            "job_run_id": job_run_id
        }

    except requests.exceptions.RequestException as e:
        print(f"Failed to get job run info: {e}")
        return None

#get latest task run Id using job run Id
def get_task_run_ids(job_run_id):
    """
    Given a Job Run ID, retrieve the task run IDs (for multi-task workflows).
    """


    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }

    # Fetch job run details
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={job_run_id}"

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to get run details: {response.status_code}")
        print(response.text)
        return None

    run_info = response.json()
    task_run_ids = []

    if "tasks" in run_info:
        #print(f"Task run info for job_run_id {job_run_id}:")
        for task in run_info["tasks"]:
            #task_key = task.get("task_key")
            task_run_id = task.get("run_id")
            #task_state = task.get("state", {}).get("result_state", "UNKNOWN")
            #print(f"  Task: {task_key}, Task Run ID: {task_run_id}, Status: {task_state}")
            #task_run_ids.append((task_key, task_run_id, task_state))
    else:
        print("No tasks found — this might be a single-task job.")

    return task_run_id 

if __name__ == "__main__":
    job_id = list_databricks_jobs()
    print("Job_Id:",job_id)
    job_run_Id = get_latest_job_run_id(job_id)
    print("Job_Run_Id:",job_run_Id.get('job_run_id'))
    Task_run_id = get_task_run_ids(job_run_Id.get('job_run_id'))
    print("Task_run_Id:",Task_run_id)