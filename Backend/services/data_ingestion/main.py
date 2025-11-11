from fastapi import FastAPI
import subprocess
import sys
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

app = FastAPI(title="Data Ingestion Service")

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts/")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

PARALLEL_SCRIPTS = ["get_company_data.py", "get_news_data.py", "get_stock_data.py"]
SEQUENTIAL_SCRIPTS = ["preprocess_data.py"]

def run_script(script_name):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    log_file = os.path.join(LOG_DIR, f"{script_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    with open(log_file, "a") as log:
        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate()
        log.write(stdout + "\n")
        if stderr:
            log.write("[ERROR]\n" + stderr)
        if process.returncode != 0:
            raise RuntimeError(f"{script_name} failed — check log {log_file}")
    return f"{script_name} completed"

@app.post("/run")
def run_ingestion():
    """Run the full ingestion pipeline"""
    start_time = datetime.now()
    results = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(run_script, s) for s in PARALLEL_SCRIPTS]
        for f in as_completed(futures):
            results.append(f.result())

    for s in SEQUENTIAL_SCRIPTS:
        results.append(run_script(s))

    total_time = datetime.now() - start_time
    return {"status": "success", "time_taken": str(total_time), "details": results}
