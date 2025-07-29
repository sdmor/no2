import json
import os
import subprocess
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time
from threading import Lock
submission_lock = Lock()

PROJECT = "no2-app"
REGION = "us-east1"
REPO = "fastapi-repo"
IMAGE = f"{REGION}-docker.pkg.dev/{PROJECT}/{REPO}/compute-ra-single"
SCRIPT = "/app/compute_ra_single_day.py"
TMP_YAML_DIR = "tmp_yaml"
MAX_PARALLEL = 5  # still used for local thread cap
MAX_RUNNING_JOBS = 5

os.makedirs(TMP_YAML_DIR, exist_ok=True)

def generate_yaml_file(date_str):
    job_name = f"compute-ra-{date_str}"
    yaml_path = os.path.join(TMP_YAML_DIR, f"{job_name}.yaml")
    with open(yaml_path, "w") as f:
        f.write(f"""\
name: {job_name}
priority: 0
taskGroups:
- taskSpec:
    runnables:
    - container:
        imageUri: {IMAGE}
        commands: ["python", "{SCRIPT}", "{date_str}"]
    computeResource:
      cpuMilli: 1000
      memoryMib: 8192
    maxRunDuration: 3600s
    maxRetryCount: 1
  taskCount: 1
allocationPolicy:
  instances:
  - policy:
      machineType: e2-standard-4
labels:
  job-type: compute-ra
logsPolicy:
  destination: CLOUD_LOGGING
""")
    return job_name, yaml_path

def job_succeeded(job_name):
    result = subprocess.run([
        "gcloud", "batch", "jobs", "describe", job_name,
        "--location", REGION,
        "--project", PROJECT
    ], capture_output=True, text=True)
    if result.returncode != 0:
        return False
    return "state: SUCCEEDED" in result.stdout

def count_running_jobs():
    result = subprocess.run(
        [
            "gcloud", "batch", "jobs", "list",
            "--location=us-east1",
            "--format=json"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if result.returncode != 0:
        print("⚠️ Failed to list jobs:", result.stderr.decode())
        return 0

    try:
        jobs = json.loads(result.stdout.decode())
        count = sum(
            1 for job in jobs
            if job["status"]["state"] in {"RUNNING", "SCHEDULED"}
        )
        return count
    except Exception as e:
        print(f"⚠️ Failed to parse job list: {e}")
        return 0

def wait_for_slot():
    while True:
        running = count_running_jobs()
        print(f"⏳ Waiting for slot... Currently {running} running/scheduled")
        if running < MAX_RUNNING_JOBS:
            return
        time.sleep(10)

import subprocess
import time

def job_exists(job_name):
    """Return True if the job still exists in GCP Batch (even if it's deleting)."""
    result = subprocess.run(
        [
            "gcloud", "batch", "jobs", "describe", job_name,
            "--location=us-east1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    return result.returncode == 0

def submit_job(date_str):
    job_name = f"compute-ra-{date_str}"
    yaml_path = f"{TMP_YAML_DIR}/{job_name}.yaml"

    # Delete existing job
    delete_result = subprocess.run(
        [
            "gcloud", "batch", "jobs", "delete", job_name,
            "--location", REGION,
            "--project", PROJECT,
            "--quiet"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if delete_result.returncode == 0:
        print(f"🗑️ Scheduled delete for {job_name} if exists, waiting...")
    elif "not found" in delete_result.stderr.decode().lower():
        print(f"✅ No existing job to delete for {job_name}")
    else:
        print(f"⚠️ Delete failed for {job_name}")
        print(delete_result.stderr.decode())
        return

    # Wait until fully deleted using job_exists()
    for _ in range(60):
        if not job_exists(job_name):
            break
        time.sleep(1)
    else:
        print(f"❌ Timeout waiting for job {job_name} to be fully deleted.")
        return

    _, yaml_path = generate_yaml_file(date_str)

    # Wait and submit job in thread-safe lock
    
    with submission_lock:
        wait_for_slot()
        print(f"🔧 Submitting job {job_name} using {yaml_path}...")
        result = subprocess.run(
            [
                "gcloud", "batch", "jobs", "submit", job_name,
                "--location", REGION,
                "--project", PROJECT,
                "--config", yaml_path
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    if result.returncode == 0:
        print(f"✅ Submitted {date_str}")
    else:
        print(f"❌ Failed {date_str}")
        print("STDOUT:\n", result.stdout.decode())
        print("STDERR:\n", result.stderr.decode())

def daterange(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

import sys

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 submit_ra_jobs.py START_DATE END_DATE MAX_PARALLEL")
        print("Example: python3 submit_ra_jobs.py 20180507 20180607 5")
        sys.exit(1)

    start_date = datetime.strptime(sys.argv[1], "%Y%m%d")
    end_date = datetime.strptime(sys.argv[2], "%Y%m%d")
    max_parallel = int(sys.argv[3])

    dates = list(daterange(start_date, end_date))
    print("Submitting the following dates:")
    for d in dates:
        print(d.strftime("%Y%m%d"))

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = []
        for d in dates:
            futures.append(executor.submit(submit_job, d.strftime("%Y%m%d")))
        for f in futures:
            f.result()  # to raise any exceptions
