from fastapi import FastAPI, Request
import subprocess
import sys
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.logging_config import setup_logger  # <-- ADD THIS

app = FastAPI(title="Data Ingestion Service")

# Initialize logger
logger = setup_logger("data_ingestion_service")

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts/")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

PARALLEL_SCRIPTS = ["get_company_data.py", "get_news_data.py", "get_stock_data.py"]
SEQUENTIAL_SCRIPTS = ["preprocess_data.py"]


# ======================================================
# Request Logging Middleware
# ======================================================
@app.middleware("http")
async def log_middle(request: Request, call_next):
    logger.info(f"Incoming request → {request.method} {request.url}")

    try:
        response = await call_next(request)
        logger.info(f"Response → status {response.status_code}")
        return response
    except Exception as e:
        logger.exception(f"Unhandled exception while processing request: {e}")
        raise


# ======================================================
# Script Runner
# ======================================================
def run_script(script_name):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    log_file = os.path.join(LOG_DIR, f"{script_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger.info(f"Starting script: {script_name}")
    logger.debug(f"Full script path: {script_path}")
    logger.debug(f"Log file: {log_file}")

    with open(log_file, "a") as log:
        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout, stderr = process.communicate()

        # Save script output to its own log file
        log.write(stdout + "\n")
        if stderr:
            log.write("[ERROR]\n" + stderr)

        # Log console info
        if stdout:
            logger.info(f"{script_name} output logged.")
        if stderr:
            logger.error(f"{script_name} error logged.")

        if process.returncode != 0:
            logger.error(f"{script_name} failed — check log {log_file}")
            raise RuntimeError(f"{script_name} failed — check log {log_file}")

    logger.info(f"Completed script: {script_name}")
    return f"{script_name} completed"


# ======================================================
# Main Ingestion Pipeline
# ======================================================
@app.post("/run")
def run_ingestion():
    logger.info("🚀 Starting ingestion pipeline")

    start_time = datetime.now()
    results = []

    try:
        # ---- PARALLEL SCRIPTS ----
        logger.info("Running parallel scripts...")
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(run_script, s) for s in PARALLEL_SCRIPTS]
            for f in as_completed(futures):
                result = f.result()
                results.append(result)
                logger.info(result)

        # ---- SEQUENTIAL SCRIPTS ----
        logger.info("Running sequential scripts...")
        for s in SEQUENTIAL_SCRIPTS:
            result = run_script(s)
            results.append(result)
            logger.info(result)

    except Exception as e:
        logger.exception(f"❌ Ingestion pipeline failed: {e}")
        return {"status": "failed", "error": str(e)}

    total_time = datetime.now() - start_time
    logger.info(f"✅ Ingestion pipeline completed in {total_time}")

    return {
        "status": "success",
        "time_taken": str(total_time),
        "details": results
    }
