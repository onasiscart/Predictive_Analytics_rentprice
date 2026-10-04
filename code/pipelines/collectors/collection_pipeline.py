import os
import re
import json
import glob
import shutil
import sys
import argparse
from datetime import datetime
from typing import List, Optional, Union, Dict, Any
import requests
from pyspark.sql import SparkSession, DataFrame

# Load basic utilities
try:
    from pipelines.utils import get_console_logger, load_config
except ImportError:
    sys.path.append(os.getcwd())
    from pipelines.utils import get_console_logger, load_config

# Global configuration and logger
logger = get_console_logger("DataCollectionPipeline")
CONFIG: Dict[str, Any] = load_config()

class DataCollector:
    """Base class providing common infrastructure for data collectors."""
    
    def __init__(self, landing_zone_path: str, spark_session: SparkSession) -> None:
        """
        Initializes the base collector with landing zone and Spark session.
        """
        self.landing_zone = landing_zone_path
        self.spark = spark_session
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.subfolders_config = CONFIG["paths"]["landing_subfolders"]
        
    def setup_directories(self) -> None:
        """Initializes the required directory structure within the landing zone."""
        for folder_name in self.subfolders_config.values():
            os.makedirs(os.path.join(self.landing_zone, folder_name), exist_ok=True)

class PriceDataCollector(DataCollector):
    """Collector of local rental price dataset into the landing zone."""

    def collect(self, source_file: str) -> bool:
        """
        Ingests a local JSON file, standardizes it via Spark, and saves it.
        """
        logger.info(f"Initiating price data ingestion from: {source_file}")
        try:
            if not os.path.exists(source_file):
                logger.error(f"Source file validation failed: {source_file} does not exist")
                return False
            
            df: DataFrame = self.spark.read.json(source_file)
            folder_name = self.subfolders_config["prices"] 
            target_dir = os.path.join(self.landing_zone, folder_name)
            temp_output = os.path.join(target_dir, f"tmp_{self.timestamp}") 
            
            df.coalesce(1).write.mode('overwrite').json(temp_output)
            
            part_files = glob.glob(os.path.join(temp_output, "part-*.json"))
            if part_files:
                final_filename = f"price_data_{self.timestamp}.json"
                final_destination = os.path.join(target_dir, final_filename)
                shutil.move(part_files[0], final_destination)
                shutil.rmtree(temp_output)
                logger.info(f"Price data successfully moved to landing zone: {final_destination}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to collect price data: {e}")
            return False

class AccidentsDataCollector(DataCollector):
    """Collector of Accidents Data from Barcelona Open Data API (CSV format)."""

    def _get_year_from_resource(self, resource: dict) -> int:
        """Parses resource metadata and returns the year of the dataset."""
        pattern = r'(20\d{2})'
        search_str = f"{resource.get('url', '')} {resource.get('name', '')}"
        match = re.search(pattern, search_str)
        return int(match.group(1)) if match else 0

    def collect(self, limit_years: Optional[int] = None) -> bool:
        """Downloads and standardizes accident datasets from the Open Data portal."""
        logger.info("Initiating accidents data collection via Open Data API")
        api_base = CONFIG["api_endpoints"]["api_base_url"]
        dataset_id = CONFIG["api_endpoints"]["accidents_dataset_id"]
        
        try:
            package_url = f"{api_base}/package_show?id={dataset_id}"
            response = requests.get(package_url, timeout=30)
            response.raise_for_status()
            
            resources = response.json().get('result', {}).get('resources', [])
            csv_list = [r for r in resources if r.get('format', '').upper() == 'CSV']
            
            if not csv_list:
                logger.warning("No CSV resources identified for the accidents dataset")
                return False

            csv_list.sort(key=self._get_year_from_resource, reverse=True)
            if limit_years:
                csv_list = csv_list[:limit_years]

            for resource in csv_list:
                year = self._get_year_from_resource(resource)
                if year == 0: continue

                year_label = f"year_{year}"
                target_folder = os.path.join(self.landing_zone, self.subfolders_config["accidents"], year_label)
                
                if glob.glob(os.path.join(target_folder, "*.csv")):
                    logger.info(f"Data for year {year} already present, skipping")
                    continue
                
                logger.info(f"Downloading dataset for year: {year}")
                csv_bytes = requests.get(resource['url']).content
                temp_filename = f"buffer_{year}.csv"
                
                with open(temp_filename, 'wb') as f:
                    f.write(csv_bytes)
                
                df: DataFrame = self.spark.read.option("header", "true").csv(temp_filename)
                
                os.makedirs(target_folder, exist_ok=True)
                tmp_spark_out = os.path.join(target_folder, "consolidating")
                df.coalesce(1).write.mode('overwrite').option("header", "true").csv(tmp_spark_out)
                
                part_file = glob.glob(os.path.join(tmp_spark_out, "part-*.csv"))[0]
                shutil.move(part_file, os.path.join(target_folder, f"accidents_{year}.csv"))
                
                shutil.rmtree(tmp_spark_out)
                os.remove(temp_filename)
            
            return True
        except Exception as e:
            logger.error(f"Accidents collection error: {e}")
            return False

class CulturalInfoCollector(DataCollector):
    """Collector of Cultural Sites using the Barcelona Datastore API."""

    def _resolve_resource_id(self) -> Optional[str]:
        """Resolves the current resource ID for the cultural dataset metadata."""
        api_base = CONFIG["api_endpoints"]["api_base_url"]
        dataset_name = CONFIG["api_endpoints"]["cultural_dataset"]
        try:
            url = f"{api_base}/package_show?id={dataset_name}"
            response = requests.get(url, timeout=30)
            data = response.json()
            if not data.get('success'): return None
            
            for resource in data['result']['resources']:
                if resource['format'].upper() in ['CSV', 'JSON']:
                    return resource['id']
            return None
        except Exception:
            return None

    def collect(self) -> bool:
        """Fetches cultural data records and saves them as a consolidated JSON file."""
        logger.info("Initiating cultural data collection")
        api_base = CONFIG["api_endpoints"]["api_base_url"]
        
        try:
            resource_id = self._resolve_resource_id()
            if not resource_id:
                logger.error("Unable to resolve a valid resource ID for cultural data")
                return False
            
            query_url = f"{api_base}/datastore_search?resource_id={resource_id}&limit=10000"
            response = requests.get(query_url, timeout=60)
            payload = response.json()
            
            if not payload.get('success'):
                logger.error("Datastore API returned an unsuccessful response")
                return False
                
            records = payload['result']['records']
            folder_name = self.subfolders_config["culture"]
            output_dir = os.path.join(self.landing_zone, folder_name)
            os.makedirs(output_dir, exist_ok=True)
            
            for existing_file in glob.glob(os.path.join(output_dir, "*.json")):
                os.remove(existing_file)
            
            output_file = os.path.join(output_dir, f"cultural_data_{self.timestamp}.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=4)
                
            logger.info(f"Cultural data snapshot saved: {output_file}")
            return True
        except Exception as e:
            logger.error(f"Cultural data collection failed: {e}")
            return False

def main() -> None:
    """Main entry point for the data collection pipeline."""
    logger.info("Data Collection Pipeline execution started")
    
    try:
        parser = argparse.ArgumentParser(description="BCN Data Collection Pipeline")
        parser.add_argument("--collector", choices=["price", "accidents", "culture", "all"], default="all")
        args = parser.parse_args()

        landing_zone = CONFIG["paths"]["landing_zone"]
        price_source = CONFIG["paths"]["price_source_file"]
        
        spark = SparkSession.builder \
            .master(CONFIG["spark"]["master"]) \
            .appName("DataCollection") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel(CONFIG["spark"]["log_level"])
        
        # Directory initialization
        base_collector = DataCollector(landing_zone, spark)
        base_collector.setup_directories()
        
        if args.collector in ["price", "all"]:
            PriceDataCollector(landing_zone, spark).collect(price_source)  
        if args.collector in ["accidents", "all"]:
            AccidentsDataCollector(landing_zone, spark).collect()   
        if args.collector in ["culture", "all"]:
            CulturalInfoCollector(landing_zone, spark).collect()
            
        spark.stop()
        logger.info("Pipeline execution finished successfully")

    except Exception as e:
        logger.critical(f"Pipeline encountered a fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()