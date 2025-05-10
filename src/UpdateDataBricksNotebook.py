import os
import requests
import base64
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def upload_notebook_to_databricks(path: str, source_code: str):
    if not path.startswith("/"):
        raise ValueError("Notebook path must start with '/'")

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    encoded_content = base64.b64encode(source_code.encode("utf-8")).decode("utf-8")

    payload = {
        "path": path,
        "format": "SOURCE",
        "language": "PYTHON",
        "overwrite": True,
        "content": encoded_content
    }

    url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        print("✅ Notebook successfully updated in Databricks.")
    else:
        print("❌ Failed to upload notebook.")
        print("Status:", response.status_code)
        print("Details:", response.text)
