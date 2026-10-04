import os
import sys
import shutil
import gc
from datetime import datetime
from typing import List, Tuple, Dict, Any
#spark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import (
    LogisticRegression, 
    RandomForestClassifier, 
    DecisionTreeClassifier
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark import StorageLevel
# MLflow imports
import mlflow
import mlflow.spark

# Environment setup to avoid problems with GitPython
os.environ['GIT_PYTHON_REFRESH'] = 'quiet'

try:
    from pipelines.utils import get_console_logger, load_config
except ImportError:
    sys.path.append(os.getcwd())
    from pipelines.utils import get_console_logger, load_config

logger = get_console_logger("MLModelPipeline")
CONFIG: Dict[str, Any] = load_config()

class MLModelPipeline:
    """
    Mchine learning workflow: data loading from Exploitation Zone,
    feature engineering, model training with hyperparameter tuning, and registration.
    """

    def __init__(self) -> None:
        """Initializes the Spark session with Delta support and MLflow tracking."""
        if not shutil.which("java"):
            logger.warning("Java not found on PATH. Spark execution may fail.")
        logger.info("Initializing Spark Session for model training...")
        self.spark: SparkSession = SparkSession.builder \
            .appName("MLModelTraining") \
            .master("local[2]") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        # Configure MLflow local tracking
        abs_mlruns: str = os.path.abspath("./mlruns") # folder in main directory
        mlflow.set_tracking_uri(f"file:{abs_mlruns}")
        mlflow.set_experiment("barcelona_price_prediction")

    def load_exploitation_data(self) -> DataFrame:
        """ Loads and retirns dataframe ready for analysis from Exploitation Zone. """
        logger.info("Loading data from Exploitation Zone...")
        path: str = CONFIG["paths"]["exploitation_zone"]
        return self.spark.read.format("delta").load(path)

    def create_target_variable(self, df: DataFrame) -> Tuple[DataFrame, Tuple[float, float]]:
        """
        Returns a DataFrame with a new target variable 'price_category', which
        is the discretized continuous price variable into three categorical classes 
        based on statistical quantiles.
        """
        logger.info("Generating 3-class target variable...")
        percentiles = df.approxQuantile("average_price_per_square_meter", [0.33, 0.67], 0.01)
        p33, p67 = percentiles[0], percentiles[1] # 33rd and 67th percentiles
        df = df.withColumn(
            "price_category",
            when(col("average_price_per_square_meter") < p33, 0.0)
            .when(col("average_price_per_square_meter") < p67, 1.0)
            .otherwise(2.0)
        )
        return df, (p33, p67) # return percentiles for reference

    def prepare_features(self, df: DataFrame) -> Tuple[Any, Any, Any, List[str]]:
        """ 
        Initializes the Spark ML pipeline stages for feature engineering:
        Returns indexer, assembler, scaler and list of names of columns used as features.
        """
        logger.info("Configuring feature transformation pipeline...")
        indexer = StringIndexer(inputCol="neighborhood_name", outputCol="neighborhood_index", handleInvalid="keep")
        feature_cols = [
            "neighborhood_index", "year", "accident_deaths", 
            "number_injured_lleus", "number_injured_greu", 
            "number_accident_victims", "number_of_accidents", 
            "number_of_cultural_sites", "average_antiquity_cultural_sites"
        ]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
        return indexer, assembler, scaler, feature_cols

    def split_data(self, df: DataFrame) -> List[DataFrame]:
        """Splits the dataset into training (70%) and validation (30%) sets"""
        return df.randomSplit([0.7, 0.3], seed=42)

    def train_logistic_regression(self, train_df: DataFrame, val_df: DataFrame) -> Tuple[PipelineModel, Dict[str, float], str]:
        """Performs Grid Search with Cross-Validation for Logistic Regression model"""
        logger.info("Training Logistic Regression...")
        with mlflow.start_run(run_name="LogisticRegression"):
            lr = LogisticRegression(featuresCol="features", labelCol="price_category", maxIter=50, family="multinomial")
            pipeline = Pipeline(stages=[lr])
            paramGrid = ParamGridBuilder() \
                .addGrid(lr.regParam, [0.01, 0.1, 1.0]) \
                .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
                .build()
            evaluator = MulticlassClassificationEvaluator(labelCol="price_category", predictionCol="prediction", metricName="accuracy")
            cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3, seed=42)
            # Fit model
            cv_model = cv.fit(train_df)
            best_model = cv_model.bestModel
            # Log best hyperparams
            mlflow.log_param("regParam", best_model.stages[-1].getRegParam())
            #preduct using best model to compute metrics
            predictions = best_model.transform(val_df)
            metrics = {"accuracy": evaluator.evaluate(predictions)}
            # Log model and best metrics
            mlflow.log_metric("accuracy", metrics["accuracy"])
            mlflow.spark.log_model(best_model, "model")
            return best_model, metrics, mlflow.active_run().info.run_id

    def train_random_forest(self, train_df: DataFrame, val_df: DataFrame) -> Tuple[PipelineModel, Dict[str, float], str]:
        """Performs Grid Search with Cross-Validation for Random Forest model"""
        logger.info("Training Random Forest...")
        with mlflow.start_run(run_name="RandomForest"):
            rf = RandomForestClassifier(featuresCol="features", labelCol="price_category", seed=42)
            pipeline = Pipeline(stages=[rf])
            paramGrid = ParamGridBuilder() \
                .addGrid(rf.numTrees, [20, 50, 100]) \
                .addGrid(rf.maxDepth, [5, 10]) \
                .build()
            evaluator = MulticlassClassificationEvaluator(labelCol="price_category", metricName="accuracy")
            cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3, seed=42)
            # Fit model
            cv_model = cv.fit(train_df)
            best_model = cv_model.bestModel
            # Predict using best model to compute metrics
            predictions = best_model.transform(val_df)
            metrics = {"accuracy": evaluator.evaluate(predictions)}
            # Log model and best metrics
            mlflow.log_metric("accuracy", metrics["accuracy"])
            mlflow.spark.log_model(best_model, "model")
            return best_model, metrics, mlflow.active_run().info.run_id

    def train_decision_tree(self, train_df: DataFrame, val_df: DataFrame) -> Tuple[PipelineModel, Dict[str, float], str]:
        """Performs Grid Search with Cross-Validation for Decision Tree model"""
        logger.info("Training Decision Tree...")
        with mlflow.start_run(run_name="DecisionTree"):
            dt = DecisionTreeClassifier(featuresCol="features", labelCol="price_category", seed=42)
            pipeline = Pipeline(stages=[dt])
            paramGrid = ParamGridBuilder().addGrid(dt.maxDepth, [5, 10, 15, 20]).build()
            evaluator = MulticlassClassificationEvaluator(labelCol="price_category", metricName="accuracy")
            cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3, seed=42)
            # Fit model
            cv_model = cv.fit(train_df)
            best_model = cv_model.bestModel
            # Predict using best model to compute metrics
            predictions = best_model.transform(val_df)
            metrics = {"accuracy": evaluator.evaluate(predictions)}
            # Log model and best metrics
            mlflow.log_metric("accuracy", metrics["accuracy"])
            mlflow.spark.log_model(best_model, "model")
            return best_model, metrics, mlflow.active_run().info.run_id

    def deploy_best_model(self, models_info: List[Tuple]) -> None:
        """
        Ranks models by accuracy and automatically promotes the best performer as
        'champion' in the MLflow Registry for production use.
        This serves as the automated deployment step.
        """
        logger.info("Evaluating all trained models for deployment...")
        # get best model
        models_info.sort(key=lambda x: x[1]["accuracy"], reverse=True)
        best_model, best_metrics, best_run_id, best_name = models_info[0]
        # deployment step
        logger.info(f"DEPLOYING: {best_name} with Accuracy: {best_metrics['accuracy']:.4f}")
        try:
            model_uri = f"runs:/{best_run_id}/model"
            # Register model in central registry
            model_details = mlflow.register_model(model_uri, "barcelona_price_predictor")
            # Assign Alias/Tag of 'champion' (deployment)
            from mlflow.tracking import MlflowClient
            client = MlflowClient()
            client.set_registered_model_alias("barcelona_price_predictor", "champion", model_details.version)
            logger.info(f"Successfully registered version {model_details.version} and deployed as 'champion'.")
        except Exception as e:
            logger.warning(f"Registration failed: {e}")

    def run(self) -> None:
        """Executes the complete machine learning lifecycle."""
        logger.info("ML Pipeline execution started.")
        try:
            # Data Loading and Preparation
            raw_df = self.load_exploitation_data()
            df_with_target, _ = self.create_target_variable(raw_df)
            idx, assem, scal, _ = self.prepare_features(df_with_target)
            # Feature processing and persistence
            logger.info("Caching prepared data for iterative training...")
            prep_pipeline = Pipeline(stages=[idx, assem, scal])
            df_prepared = prep_pipeline.fit(df_with_target).transform(df_with_target)
            df_prepared.persist(StorageLevel.MEMORY_AND_DISK)
            # Split data
            train_df, val_df = self.split_data(df_prepared)
            # Train three models with hyperparameter tuning
            results = []
            results.append((*self.train_logistic_regression(train_df, val_df), "Logistic Regression"))
            gc.collect()
            results.append((*self.train_random_forest(train_df, val_df), "Random Forest"))
            gc.collect()
            results.append((*self.train_decision_tree(train_df, val_df), "Decision Tree"))
            # Select and deploy best model
            self.deploy_best_model(results)
            # Cleanup
            df_prepared.unpersist()
            logger.info("ML Pipeline completed successfully.")    
        except Exception as e:
            logger.error(f"Execution error: {str(e)}", exc_info=True)
            raise e
        finally:
            self.spark.stop()

if __name__ == "__main__":
    try:
        MLModelPipeline().run()
    except Exception:
        sys.exit(1)