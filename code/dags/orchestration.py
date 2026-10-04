import os
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# 1. SETUP AND CONFIGURATION

# Dynamic path configuration to locate project root
current_dag_path = os.path.realpath(__file__)
PROJECT_PATH = os.path.dirname(os.path.dirname(current_dag_path))

# Ensure pipelines.utils can be imported by adding project root to sys.path
if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

from pipelines.utils import load_config, get_console_logger

# Load global configuration from config.yaml
CONFIG = load_config()
LANDING_PATH = CONFIG["paths"]["landing_zone"]
CULTURE_SUBFOLDER = CONFIG["paths"]["landing_subfolders"]["culture"]
ACCIDENTS_SUBFOLDER = CONFIG["paths"]["landing_subfolders"]["accidents"]

# Helper functions for scheduling and logging
def data_exists(subfolder: str) -> bool:
    """
    Checks if data is available both in the local landing zone and inside MongoDB.
    This ensures the pipeline doesn't skip tasks if the database was wiped.
    """
    # 1. Quick check for local files in the landing zone
    local_path = os.path.join(PROJECT_PATH, LANDING_PATH, subfolder)
    has_local_files = os.path.exists(local_path) and any(os.scandir(local_path))
    
    if not has_local_files:
        return False

    # 2. Check if MongoDB actually has data for this specific category
    try:
        # Connect using the URI from config (timeout set to 2s to avoid hanging)
        client = MongoClient(CONFIG["database"]["mongo_uri"], serverSelectionTimeoutMS=2000)
        db = client[CONFIG["database"]["db_name"]]
        
        # Map folder names to their respective MongoDB collections
        mapping = {
            CONFIG["paths"]["landing_subfolders"]["prices"]: CONFIG["database"]["collections"]["prices"],
            CONFIG["paths"]["landing_subfolders"]["accidents"]: CONFIG["database"]["collections"]["accidents"],
            CONFIG["paths"]["landing_subfolders"]["culture"]: CONFIG["database"]["collections"]["culture"]
        }
        
        collection_name = mapping.get(subfolder)
        if not collection_name:
            return False
            
        # Check if the collection contains at least one document
        return db[collection_name].count_documents({}, limit=1) > 0
            
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        # If DB is down, we return False to force the pipeline to run and retry
        logger = get_console_logger("Airflow_Scheduler")
        logger.warning(f"Mongo connection failed during check: {e}")
        return False
    finally:
        client.close()

def is_weekly_run() -> bool:
    """Returns True iff the culture pipeline should execute based on weekday or data absence"""
    logger = get_console_logger("Airflow_Scheduler")
    condition = (datetime.now().weekday() == 0) or (not data_exists(CULTURE_SUBFOLDER))
    if condition:
        logger.info("Schedule check: Proceeding with Culture pipeline.")
    else:
        logger.info("Schedule check: Skipping Culture pipeline (weekly condition not met).")
    return condition

def is_annual_run() -> bool:
    """Returns True iff the accidents pipeline should execute based on date (Jan 1st) or data absence."""
    logger = get_console_logger("Airflow_Scheduler")
    is_jan_1 = (datetime.now().month == 1 and datetime.now().day == 1)
    condition = is_jan_1 or (not data_exists(ACCIDENTS_SUBFOLDER))
    if condition:
        logger.info(is_jan_1)
        logger.info(condition)
        logger.info("Schedule check: Proceeding with Accidents pipeline.")
    else:
        logger.info("Schedule check: Skipping Accidents pipeline (annual condition not met).")
    return condition

def log_start_pipeline():
    """Logs the execution start in the centralized project log file."""
    logger = get_console_logger("Airflow_Scheduler")
    logger.info("-" * 60)
    logger.info(f"AIRFLOW: STARTING DAG EXECUTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("-" * 60)

# 2. DAG DEFINITION

default_args = {
    'owner': 'Team11I',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bcn_rental_prediction_pipeline',
    default_args=default_args,
    description='End-to-End Pipeline to Predict Rental Prices in Barcelona',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['UPC', 'BDA', 'DE_Project'],
) as dag:
    
    # Start marker task
    start_marker = PythonOperator(
        task_id='mark_execution_start',
        python_callable=log_start_pipeline
    )
    # Price data tasks
    collect_prices = BashOperator(
        task_id='collect_prices',
        bash_command=f'cd {PROJECT_PATH} && python pipelines/collectors/collection_pipeline.py --collector price'
    )
    format_prices = BashOperator(
        task_id='format_prices',
        bash_command=f'cd {PROJECT_PATH} && python pipelines/formatters/formatting_pipeline.py --formatter prices'
    )
    # Cultural data tasks (Weekly or if no data)
    check_culture_schedule = ShortCircuitOperator(
        task_id='check_culture_schedule',
        python_callable=is_weekly_run,
        ignore_downstream_trigger_rules=False 
    )
    collect_culture = BashOperator(
        task_id='collect_culture',
        bash_command=f'cd {PROJECT_PATH} && python pipelines/collectors/collection_pipeline.py --collector culture'
    )
    format_culture = BashOperator(
        task_id='format_culture',
        bash_command=f'cd {PROJECT_PATH} && python pipelines/formatters/formatting_pipeline.py --formatter culture'
    )
    # Accident data tasks (Annual or if no data)
    check_accidents_schedule = ShortCircuitOperator(
        task_id='check_accidents_schedule',
        python_callable=is_annual_run,
        ignore_downstream_trigger_rules=False
    )
    collect_accidents = BashOperator(
        task_id='collect_accidents',
        bash_command=f'cd {PROJECT_PATH} && python pipelines/collectors/collection_pipeline.py --collector accidents'
    )
    format_accidents = BashOperator(
        task_id='format_accidents',
        bash_command=f'cd {PROJECT_PATH} && python pipelines/formatters/formatting_pipeline.py --formatter accidents'
    )
    # Transformation
    transform_exploitation = BashOperator(
        task_id='create_exploitation_zone',
        bash_command=f'cd {PROJECT_PATH} && python pipelines/transformers/transformer_pipeline.py',
        trigger_rule=TriggerRule.NONE_FAILED 
    )
    # ML Model Training and Evaluation
    train_model = BashOperator(
        task_id='train_and_evaluate_model',
        bash_command=f'cd {PROJECT_PATH} && python pipelines/models/ml_model_pipeline.py',
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    # PIPELINE DEPENDENCIES
    start_marker >> [collect_prices, check_culture_schedule, check_accidents_schedule]
    # data engineering backbone (can be concurrent)
    collect_prices >> format_prices
    check_culture_schedule >> collect_culture >> format_culture
    check_accidents_schedule >> collect_accidents >> format_accidents
    # transformation and data analysis backbone
    [format_prices, format_culture, format_accidents] >> transform_exploitation >> train_model