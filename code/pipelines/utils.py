import logging
import os
import yaml
from logging.handlers import RotatingFileHandler
from typing import Dict, Any

def get_console_logger(logger_name: str) -> logging.Logger:
    """
    Initializes a logger with logger_name and returns the configured logging.Logger object.
    """
    logger = logging.getLogger(logger_name)
    
    # Avoid duplicate handlers if logger is initialized multiple times
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # StreamHandler for console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # RotatingFileHandler for local persistence
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 5MB limit per file with 3 backup rotations to prevent disk overflow
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "pipeline_execution.log"), 
        maxBytes=5*1024*1024, 
        backupCount=3
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Parses YAML config file and returns configuration dictionary, with fallback values if file is missing.
    """
    # fallback to ensure pipeline remains functional without config file
    default_config = {
        "paths": {
            "landing_zone": "data/landing_zone",
            "price_source_file": "data/raw/prices.json"
        },
        "database": {
            "mongo_uri": "mongodb://localhost:27017",
            "db_name": "formatted_zone",
            "collections": {
                "prices": "price_data",
                "accidents": "accident_data",
                "culture": "cultural_data"
            }
        },
        "spark": {
            "master": "local[*]", 
            "log_level": "WARN",
            "mongodb_package": "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
        }
    }
    if not os.path.exists(config_path):
        logging.warning(f"Configuration file {config_path} not found. Using internal defaults.")
        return default_config    
    try:
        with open(config_path, "r") as f:
            user_config = yaml.safe_load(f)
            return user_config if user_config else default_config
    except Exception as e:
        logging.error(f"Failed to load configuration at {config_path}: {e}")
        raise e