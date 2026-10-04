import os
import sys
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lower, trim, count, sum, avg, 
    coalesce, lit, current_date, datediff, max as spark_max
)

# Load basic utilities
try:
    from pipelines.utils import get_console_logger, load_config
except ImportError:
    sys.path.append(os.getcwd())
    from pipelines.utils import get_console_logger, load_config

# Initialize Logger and Load Configuration
logger = get_console_logger("ExploitationPipeline")
CONFIG: Dict[str, Any] = load_config()

class ExploitationPipeline:
    """
    Extracts data from MongoDB collections, transforms it for price prediction analysis and loads into Exploitation Zone (Delta Lake).
    """
    
    def __init__(self) -> None:
        """
        Initializes the Spark session with MongoDB and Delta Lake configurations.
        """
        mongo_uri: str = CONFIG["database"]["mongo_uri"]
        self.spark: SparkSession = SparkSession.builder \
            .appName("ExploitationZone") \
            .master(CONFIG["spark"]["master"]) \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.mongodb.read.connection.uri", mongo_uri) \
            .config("spark.mongodb.write.connection.uri", mongo_uri) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def read_mongo_collection(self, collection_name: str) -> DataFrame:
        """
        Loads and returns a DataFrame from a specified MongoDB collection "collection_name".
        """
        db_name: str = CONFIG["database"]["db_name"]
        return self.spark.read \
            .format("mongodb") \
            .option("database", db_name) \
            .option("collection", collection_name) \
            .load()

    def process_prices(self) -> DataFrame:
        """
        Averages residential prices per square meter by neighborhood and year.
        Returns a DataFrame with columns: join_neigh, year, average_price_per_square_meter.
        """
        logger.info("Processing residential price data")
        coll_name = CONFIG["database"]["collections"]["prices"] # read price collection name from config
        df: DataFrame = self.read_mongo_collection(coll_name)
        df = df.withColumn("year", col("year").cast("int"))
        df = df.withColumn("join_neigh", lower(trim(col("neighborhood_name"))))
        return df.groupBy("join_neigh", "year") \
            .agg(avg("price_per_meter").alias("average_price_per_square_meter"))

    def process_accidents(self) -> DataFrame:
        """
        Aggregates accident data by neighborhood and year.
        Returns a DataFrame containing 'neighborhood_code', 'number_of_accidents', and the sum of victims/injuries, 
        indexed by 'join_neigh' and 'year'.
        """
        logger.info("Processing urban accidents data")
        coll_name = CONFIG["database"]["collections"]["accidents"]
        df: DataFrame = self.read_mongo_collection(coll_name) 
        df = df.withColumn("join_neigh", lower(trim(col("neighborhood_name"))))
        df = df.withColumn("year", col("year").cast("int"))
        neigh_code_col = col("neighborhood_id") if "neighborhood_id" in df.columns else lit(None)
        return df.groupBy("join_neigh", "year") \
            .agg(
                spark_max(neigh_code_col).alias("neighborhood_code"),
                count("*").alias("number_of_accidents"),
                sum("num_victims").alias("number_accident_victims"),
                sum(coalesce(col("num_deaths"), lit(0))).alias("accident_deaths"),
                sum(coalesce(col("num_minor_injuries"), lit(0))).alias("number_injured_lleus"),
                sum(coalesce(col("num_serious_injuries"), lit(0))).alias("number_injured_greu")
            )

    def process_cultural(self) -> DataFrame:
        """
        Calculates cultural bsite columns: average antiquity and number of sites per neighborhood.
        Returns a DataFrame with 'number_of_cultural_sites' and 'average_antiquity_cultural_sites', 
        grouped by 'join_neigh'.
        """
        logger.info("Processing cultural venues and heritage data")
        coll_name = CONFIG["database"]["collections"]["culture"]
        df: DataFrame = self.read_mongo_collection(coll_name)
        # handle schema variations to unify neighborhood column name
        if "addresses_neighborhood_name" in df.columns:
            df = df.withColumn("join_neigh", lower(trim(col("addresses_neighborhood_name"))))
        else:
            df = df.withColumn("join_neigh", lower(trim(col("neighborhood_name"))))
        # calculate antiquity in years
        if "created" in df.columns:
            df = df.withColumn("antiquity_years", 
                               (datediff(current_date(), col("created").cast("date")) / 365))
        else:
            df = df.withColumn("antiquity_years", lit(0))
        # aggregate by neighborhood: store count and average antiquity
        return df.groupBy("join_neigh") \
            .agg(
                count("*").alias("number_of_cultural_sites"),
                avg(coalesce(col("antiquity_years"), lit(0))).alias("average_antiquity_cultural_sites")
            )

    def run(self) -> None:
        """
        Execute Transfromer to:
        1. Extract data from MongoDB
        2. Apply analysis-specific transformations
        3. Load results into Delta Lake Exploitation Zone
        """
        logger.info("Exploitation pipeline execution sequence started")
        try:
            # Process source dataframes
            df_prices: DataFrame = self.process_prices()
            df_accidents: DataFrame = self.process_accidents()
            df_cultural: DataFrame = self.process_cultural()
            # Consolidation using neighborhood and year keys
            logger.info("Merging processed datasets into a unified model")
            final_df: DataFrame = df_prices.alias("p") \
                .join(df_accidents.alias("a"), 
                      (col("p.join_neigh") == col("a.join_neigh")) & 
                      (col("p.year") == col("a.year")), 
                      "left")
            final_df = final_df.join(df_cultural.alias("c"), 
                                     col("p.join_neigh") == col("c.join_neigh"), 
                                     "left")
            # Final column selection and handling missing values
            output_df: DataFrame = final_df.select(
                col("p.join_neigh").alias("neighborhood_name"),
                col("p.year").alias("year"),
                coalesce(col("a.neighborhood_code"), lit("UNKNOWN")).alias("neighborhood_code"),
                coalesce(col("a.accident_deaths"), lit(0)).alias("accident_deaths"),
                coalesce(col("a.number_injured_lleus"), lit(0)).alias("number_injured_lleus"),
                coalesce(col("a.number_injured_greu"), lit(0)).alias("number_injured_greu"),
                coalesce(col("a.number_accident_victims"), lit(0)).alias("number_accident_victims"),
                coalesce(col("a.number_of_accidents"), lit(0)).alias("number_of_accidents"),
                coalesce(col("c.number_of_cultural_sites"), lit(0)).alias("number_of_cultural_sites"),
                coalesce(col("c.average_antiquity_cultural_sites"), lit(0)).alias("average_antiquity_cultural_sites"),
                col("p.average_price_per_square_meter").alias("average_price_per_square_meter")
            )
            # Persistence to Delta Lake
            output_path: str = CONFIG["paths"]["exploitation_zone"]
            logger.info(f"Writing exploitation results to Delta Lake path: {output_path}")
            output_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy("year") \
                .save(output_path)
            logger.info("Exploitation pipeline task finished successfully")
            
        except Exception as e:
            logger.error(f"Execution failed during exploitation phase: {e}")
            raise e
        finally:
            self.spark.stop()

if __name__ == "__main__":
    logger.info("Transformer initiated")
    try:
        pipeline = ExploitationPipeline()
        pipeline.run()
    except Exception as e:
        logger.error(f"Terminal script error: {e}")
        sys.exit(1)