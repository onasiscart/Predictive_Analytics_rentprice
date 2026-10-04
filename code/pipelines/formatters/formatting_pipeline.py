import os
import sys
import json
import argparse
import glob
import re
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, explode, explode_outer, concat_ws, lit, input_file_name, 
    regexp_extract, max as spark_max
)

# Load basic utilities
try:
    from pipelines.utils import get_console_logger, load_config
except ImportError:
    sys.path.append(os.getcwd())
    from pipelines.utils import get_console_logger, load_config

# Global configuration and logger
logger = get_console_logger("FormattersPipeline")
CONFIG: Dict[str, Any] = load_config()

# Helper functions for incremental processing
def get_processed_metadata(metadata_path: str) -> Dict[str, Any]:
    """
    Reads the metadata tracking JSON to identify already processed files.
    Returns a dictionary of filename: metadata_object.
    """
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def update_processing_metadata(metadata_path: str, filename: str, meta_value: Any) -> None:
    """
    Updates the metadata tracking JSON file "filename" in "metadata_path" folder after a successful file process.
    """
    metadata = get_processed_metadata(metadata_path)
    metadata[filename] = meta_value
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=4)

class PricesFormatter:
    """Handles rental price data, flattening nested JSONs into MongoDB."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self) -> None:
        """Process price files incrementally based on last year seen in metadata."""
        logger.info("Checking for new price data...")
        landing_path = CONFIG["paths"]["landing_zone"]
        subfolder = CONFIG["paths"]["landing_subfolders"]["prices"]
        input_dir = os.path.join(landing_path, subfolder)
        metadata_file = os.path.join(landing_path, CONFIG["metadata"]["prices"])
        
        all_files = glob.glob(os.path.join(input_dir, "*.json"))
        history = get_processed_metadata(metadata_file)
        
        for f_path in all_files:
            fname = os.path.basename(f_path)
            f_mtime = os.path.getmtime(f_path) 
            # Use mtime to skip files that haven't changed at all
            file_meta = history.get(fname, {"mtime": 0, "last_year": 0})
            if f_mtime <= file_meta["mtime"]:
                continue
            logger.info(f"New content detected in {fname}")
            
            # multiLine JSON
            df: DataFrame = self.spark.read.option("multiLine", "true").json(f_path)
            # Some files have a trailing space in the neighborhood column name
            if "neigh_name " in df.columns:
                df = df.withColumnRenamed("neigh_name ", "neighborhood_name")
            elif "neigh_name" in df.columns:
                df = df.withColumnRenamed("neigh_name", "neighborhood_name")
            # Flatten the 'info' array and cast metrics to proper types
            df_final = df.withColumn("info_exploded", explode(col("info"))).select(
                col("_id").alias("neighborhood_id").cast("int"),
                col("district_id").cast("int"),
                col("district_name"),
                col("neighborhood_name"),
                col("info_exploded.year").alias("year").cast("int"),
                col("info_exploded.PerMeter").alias("price_per_meter").cast("double"),
                col("info_exploded.Amount").alias("total_amount").cast("double"),
                col("info_exploded.usedAmount").alias("used_amount").cast("double"),
                col("info_exploded.diffAmount").alias("diff_amount").cast("double"),
                col("info_exploded.diffPerMeter").alias("diff_price_per_meter").cast("double")
            ).filter(col("year") > lit(file_meta["last_year"])) # Only grab new years
            if df_final.count() > 0:
                # Composite ID (ID-Year) to avoid duplicates in the same collection
                df_final.withColumn("_id", concat_ws("-", col("neighborhood_id"), col("year"))).write \
                    .format("mongodb").mode("append") \
                    .option("database", CONFIG["database"]["db_name"]) \
                    .option("collection", CONFIG["database"]["collections"]["prices"]) \
                    .option("replaceDocument", "true").save()
                # Update tracking with the latest year processed
                new_max_year = df_final.agg(spark_max("year")).collect()[0][0]
                update_processing_metadata(metadata_file, fname, {"mtime": f_mtime, "last_year": new_max_year})
                logger.info(f"Finished {fname}. Latest year: {new_max_year}")
            else:
                # If mtime changed but no new years were added, just update mtime
                update_processing_metadata(metadata_file, fname, {"mtime": f_mtime, "last_year": file_meta["last_year"]})
                logger.info(f"No new years found in {fname}")

class AccidentsFormatter:
    """Handles ingestion of accident CSVs with inconsistent annual schemas."""
    
    def __init__(self, spark: SparkSession):
        """Initializes the formatter with a Spark session."""
        self.spark = spark
        #define column mapping for normalization in Formatted zone
        self.column_mapping = {
            "Numero_expedient": "expedient_id", "Nmero d'expedient": "expedient_id",
            "Número d'expedient": "expedient_id", "Nmero d expedient": "expedient_id",
            "NK Any": "year", "NK_Any": "year", "Any": "year",
            "Codi_districte": "district_id", "Codi districte": "district_id",
            "Nom_districte": "district_name", "Nom districte": "district_name",
            "Codi_barri": "neighborhood_id", "Codi barri": "neighborhood_id",
            "Nom_barri": "neighborhood_name", "Nom barri": "neighborhood_name",
            "Numero_victimes": "num_victims", "Nmero de vctimes": "num_victims",
            "Numero_morts": "num_deaths", "Numero_lesionats_lleus": "num_minor_injuries",
            "Numero_lesionats_greus": "num_serious_injuries"
        }

    def _normalize(self, df: DataFrame, file_path: str) -> DataFrame:
        """Standardizes schema across different yearly accident reports to have consistent formatted zone"""
        select_exprs = []
        found = set()
        # Map columns based on known variations
        for c in df.columns:
            clean = c.strip()
            target = next((v for k, v in self.column_mapping.items() if k in clean), None)
            if target and target not in found:
                select_exprs.append(col(c).alias(target))
                found.add(target)
            elif not target:
                safe_name = clean.replace(" ", "_").replace("'", "")
                select_exprs.append(col(c).alias(safe_name))
        df_res = df.select(*select_exprs)
        # Ensure 'year' column exists
        if "year" not in df_res.columns:
            year_val = re.search(r'(\d{4})', os.path.basename(file_path))
            if year_val:
                df_res = df_res.withColumn("year", lit(int(year_val.group(1))))
            else:
                df_res = df_res.withColumn("year", regexp_extract(input_file_name(), r"year_(\d{4})", 1).cast("int"))
        return df_res

    def run(self) -> None:
        """Processes accident files incrementally (new files only)"""
        logger.info("Starting incremental accidents formatting")
        landing_path = CONFIG["paths"]["landing_zone"]
        metadata_file = os.path.join(landing_path, CONFIG["metadata"]["accidents"])
        subfolder = CONFIG["paths"]["landing_subfolders"]["accidents"]
        all_files = glob.glob(os.path.join(landing_path, subfolder, "**", "*.csv"), recursive=True)
        history = get_processed_metadata(metadata_file)
        to_process = [
            f for f in all_files 
            if os.path.basename(f) not in history or os.path.getmtime(f) > history[os.path.basename(f)]
        ]
        if not to_process:
            logger.info("Accidents data is already up to date")
            return
        #process files one by one to isolate errors
        for f_path in to_process:
            try:
                line_sample = self.spark.read.text(f_path).first()[0]
                delimiter = ";" if line_sample.count(";") > line_sample.count(",") else ","
                df_raw = self.spark.read.option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("delimiter", delimiter) \
                    .option("encoding", "ISO-8859-1") \
                    .csv(f_path)
                df_final = self._normalize(df_raw, f_path).dropDuplicates(["expedient_id"])
                df_final.withColumn("_id", col("expedient_id").cast("string")).write \
                    .format("mongodb").mode("append") \
                    .option("database", CONFIG["database"]["db_name"]) \
                    .option("collection", CONFIG["database"]["collections"]["accidents"]) \
                    .option("replaceDocument", "true").save()
                update_processing_metadata(metadata_file, os.path.basename(f_path), os.path.getmtime(f_path))
            except Exception as e:
                logger.error(f"Failed to process accident file {f_path}: {e}")

class CulturalFormatter:
    """Processes points of interest (cultural info) from landing to MongoDB."""
    
    def __init__(self, spark: SparkSession):
        """Initializes the formatter with a Spark session."""
        self.spark = spark

    def run(self) -> None:
        """Transforms cultural data file using incremental file tracking."""
        logger.info("Starting incremental cultural data formatting")
        landing_path = CONFIG["paths"]["landing_zone"]
        subfolder = CONFIG["paths"]["landing_subfolders"]["culture"]
        input_dir = os.path.join(landing_path, subfolder)
        metadata_file = os.path.join(landing_path, CONFIG["metadata"]["culture"])
        all_files = glob.glob(os.path.join(input_dir, "*.json"))
        history = get_processed_metadata(metadata_file)
        to_process = [ #process only new or modified files
            f for f in all_files 
            if os.path.basename(f) not in history or os.path.getmtime(f) > history[os.path.basename(f)]
        ]
        if not to_process:
            logger.info("Cultural data is already up to date")
            return
        #process files (should be only one)
        for f_path in to_process:
            try:
                df = self.spark.read.option("multiline", "true").json(f_path)
                # normalize columns 
                if "addresses" in df.columns:
                    df = df.withColumn("addr", explode_outer(col("addresses"))) \
                           .withColumn("neighborhood_name", col("addr.neighborhood_name")).drop("addresses", "addr")
                if "name" in df.columns:
                    df = df.withColumnRenamed("name", "entity_name")   
                df = df.dropDuplicates(["register_id"])
                df.withColumn("_id", col("register_id").cast("string")).write \
                    .format("mongodb").mode("append") \
                    .option("database", CONFIG["database"]["db_name"]) \
                    .option("collection", CONFIG["database"]["collections"]["culture"]) \
                    .option("replaceDocument", "true").save()
                update_processing_metadata(metadata_file, os.path.basename(f_path), os.path.getmtime(f_path))
                logger.info(f"Successfully processed cultural file: {os.path.basename(f_path)}")
            except Exception as e:
                logger.error(f"Error processing cultural file {f_path}: {e}")

def main() -> None:
    logger.info("Data formatting pipeline execution started")
    # parse input arguments to know which formatters to run
    parser = argparse.ArgumentParser()
    parser.add_argument("--formatter", choices=["all", "prices", "accidents", "culture"], default="all")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("BcnFormattingPipeline") \
        .master(CONFIG["spark"]["master"]) \
        .config("spark.mongodb.write.connection.uri", CONFIG["database"]["mongo_uri"]) \
        .config("spark.jars.packages", CONFIG["spark"].get("mongodb_package")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #run selected formatters
    try:
        if args.formatter in ["all", "accidents"]:
            AccidentsFormatter(spark).run()
        if args.formatter in ["all", "culture"]:
            CulturalFormatter(spark).run()
        if args.formatter in ["all", "prices"]:
            PricesFormatter(spark).run()    
        logger.info("Formatting pipeline finished successfully") 
    except Exception as e:
        logger.critical(f"Formatting script failure: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()