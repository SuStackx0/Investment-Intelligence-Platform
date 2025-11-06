import subprocess
import sys
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------------
# CONFIG
# -------------------------------
PROJECT_ROOT = "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend"
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "data_cleaning", "scripts")

PARALLEL_SCRIPTS = [
    "get_company_data.py",
    "get_news_data.py",
    "get_stock_data.py"
]

SEQUENTIAL_SCRIPTS = [
    "preprocess_data.py",
    "embed_data.py"
]

LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# -------------------------------
# HELPER FUNCTION
# -------------------------------
def run_script(script_name):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    print(f"\n🚀 Running {script_name} ...")
    with open(LOG_FILE, "a") as log:
        log.write(f"\n\n=== Running {script_name} @ {datetime.now()} ===\n")

        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate()

        # log everything
        log.write(stdout)
        if stderr:
            log.write("\n[ERROR OUTPUT]\n" + stderr)

        if process.returncode != 0:
            print(f"❌ {script_name} failed! Check logs in {LOG_FILE}")
            print(stderr)
            sys.exit(1)

    print(f"✅ {script_name} completed successfully.")
    return script_name

# -------------------------------
# MAIN PIPELINE
# -------------------------------
if __name__ == "__main__":
    print("🔄 Starting Full Investment Data Pipeline with 4 Workers\n")
    start_time = datetime.now()

    # --- Step 1: Run first 3 in parallel ---
    print("⚙️ Running parallel data fetch scripts (company, news, stock)...")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_script, script) for script in PARALLEL_SCRIPTS]
        for future in as_completed(futures):
            script_done = future.result()
            print(f"✅ Finished {script_done}")

    # --- Step 2: Run merge + embed sequentially ---
    print("\n🔗 Running merge + embed sequentially...")
    for script in SEQUENTIAL_SCRIPTS:
        run_script(script)

    total_time = datetime.now() - start_time
    print(f"\n🎉 Pipeline completed successfully in {total_time}.")
    print(f"📝 Logs saved at: {LOG_FILE}")
