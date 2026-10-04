"""script to trigger and monitor the Airflow DAG for the Barcelona rental price prediction pipeline"""
import os
import subprocess
import json
import time
from datetime import datetime
from typing import Optional, List, Dict, Any

# ---- Project Configuration (Relative) ----
BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))
os.environ["AIRFLOW_HOME"] = os.path.join(BASE_DIR, "airflow_home")
os.environ["PYTHONWARNINGS"] = "ignore"

DAG_ID: str = "bcn_rental_prediction_pipeline"

def invoke_airflow(args: List[str]) -> Optional[str]:
    """Executes an Airflow CLI command and captures the output."""
    try:
        cmd = ["airflow"] + args
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return proc.stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

def fetch_failure_details() -> str:
    """Finds and reads the last modified error log in the Airflow directory."""
    try:
        # Search for the most recently modified log file in the logs directory
        log_dir = os.path.join(BASE_DIR, "airflow_home", "logs")
        cmd = f"find {log_dir} -name '*.log' -type f -printf '%T@ %p\\n' | sort -n | tail -1 | cut -f2- -d' '"
        log_path = subprocess.check_output(cmd, shell=True, text=True).strip()
        
        if log_path and os.path.exists(log_path):
            with open(log_path, 'r') as f:
                # Get last 15 lines of the log for context
                return f.readlines()[-15:]
        return ["No specific error logs found."]
    except Exception:
        return ["Could not retrieve error details automatically."]

def get_latest_run_info(dag_id: str, target_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Retrieves metadata for the specific triggered run or the most recent one."""
    raw_output = invoke_airflow(["dags", "list-runs", "-d", dag_id, "-o", "json"])
    if not raw_output:
        return None
    try:
        runs = json.loads(raw_output)
        if not runs:
            return None 
        if target_id:
            match = next((r for r in runs if r['run_id'] == target_id), None)
            if match: return match
        
        # Sort by execution date to ensure we monitor the newest instance
        runs.sort(key=lambda x: x['execution_date'])
        return runs[-1]
    except json.JSONDecodeError:
        return None

def main() -> None:
    print(f"{'='*70}\n ORCHESTRATOR: Starting {DAG_ID}\n{'='*70}")
    
    # Ensure DAG is unpaused
    invoke_airflow(["dags", "unpause", DAG_ID])
    
    # Triggering the run
    print("[*] Triggering new DAG run...")
    trigger_raw = invoke_airflow(["dags", "trigger", DAG_ID, "-o", "json"])
    
    current_run_id: Optional[str] = None
    try:
        if trigger_raw:
            trigger_data = json.loads(trigger_raw)
            current_run_id = trigger_data[0]['run_id']
            print(f"[OK] Run successfully created: {current_run_id}")
    except:
        print("[!] Active or recent run detected. Connecting to latest instance...")

    # Monitoring loop
    while True:
        run_info = get_latest_run_info(DAG_ID, current_run_id)
        if not run_info:
            print("[...] Syncing with Airflow database...")
            time.sleep(5)
            continue
            
        rid = run_info['run_id']
        status = run_info['state'].upper()
        
        # UI Refresh
        os.system('clear' if os.name == 'posix' else 'cls')
        print(f"{'='*85}")
        print(f" BDA PIPELINE MONITOR | SYSTEM TIME: {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*85}")
        print(f"RUN ID: {rid}")
        print(f"GLOBAL STATUS: {status}")
        print(f"{'-'*85}")

        # Task detail reporting
        tasks_raw = invoke_airflow(["tasks", "states-for-dag-run", DAG_ID, rid])
        if tasks_raw:
            print(f"{'TASK ID':<35} | {'STATE':<12} | {'START TIME'}")
            print(f"{'-'*85}")
            
            for line in tasks_raw.splitlines():
                if '|' in line and 'dag_id' not in line:
                    cols = [c.strip() for c in line.split('|')]
                    if len(cols) > 4:
                        print(f"{cols[2]:<35} | {cols[3]:<12} | {cols[4]}")

        # Exit conditions and diagnostics
        if status == 'SUCCESS':
            print(f"{'-'*85}\n[FINISH] Pipeline executed successfully.\n")
            break
        elif status == 'FAILED':
            print(f"{'-'*85}\n[ERROR] Pipeline failed. Extracting task logs...\n")
            error_lines = fetch_failure_details()
            print("--- LAST LOG ENTRIES ---")
            for err in error_lines:
                print(f"  > {err.strip()}")
            print(f"{'-'*85}\n")
            break

        time.sleep(10) # Update frequency

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user. Monitoring closed.")