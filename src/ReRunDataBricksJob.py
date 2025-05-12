import os
import requests
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def rerun_databricks_job(job_id: int):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    payload = {
        "job_id": job_id
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        run_id = response.json().get("run_id")
        print(f"✅ Job re-run started successfully. Run ID: {run_id}")
        return run_id
    else:
        print("❌ Failed to re-run job:", response.status_code)
        print(response.text)
        return None
