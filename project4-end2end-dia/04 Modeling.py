# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Using the silver delta table(s) that were setup by your ETL module train and validate your token recommendation engine. Split, Fit, Score, Save
# MAGIC - Log all experiments using mlflow
# MAGIC - capture model parameters, signature, training/test metrics and artifacts
# MAGIC - Tune hyperparameters using an appropriate scaling mechanism for spark.  [Hyperopt/Spark Trials ](https://docs.databricks.com/_static/notebooks/hyperopt-spark-ml.html)
# MAGIC - Register your best model from the training run at **Staging**.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Your Code starts here...

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from delta.tables import *
import random
 
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec
 
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# COMMAND ----------

triplets = spark.sql("select * from g02_db.final_triplets_silver").cache()
valid_token_meta = spark.sql("select * from g02_db.valid_token_meta_silver").cache()

# COMMAND ----------

seed = 42
(split_60_df, split_a_20_df, split_b_20_df) = triplets.randomSplit([0.6, 0.2, 0.2], seed=seed)
 
training_df = split_60_df.cache()
validation_df = split_a_20_df.cache()
test_df = split_b_20_df.cache()
 
print('Training: {0}, validation: {1}, test: {2}\n'.format(
  training_df.count(), validation_df.count(), test_df.count())
)
training_df.show(3)
validation_df.show(3)
test_df.show(3)

# COMMAND ----------

training_df = training_df.withColumn("interacted_count", training_df["interacted_count"].cast(DoubleType())).cache()
validation_df = validation_df.withColumn("interacted_count", validation_df["interacted_count"].cast(DoubleType())).cache()
test_df = test_df.withColumn("interacted_count", test_df["interacted_count"].cast(DoubleType())).cache()

# COMMAND ----------

reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="interacted_count", metricName="rmse")

# COMMAND ----------

def train_als(regParam, rank):
    with mlflow.start_run(run_name="G02_experiment", nested=True):
        als = ALS()
        (als.setMaxIter(10)
           .setSeed(seed)
           .setItemCol("tokenId")
           .setRatingCol("interacted_count")\
           .setUserCol("walletId")
           .setColdStartStrategy("drop")
           .setRegParam(regParam)
           .setRank(rank)
        )
        
        mlflow.log_param("regParam", regParam)
        mlflow.log_param("rank", rank)
        
        model = als.fit(training_df)
        
        predicted_train_df = model.transform(training_df)
        predicted_val_df = model.transform(validation_df)
        
        predicted_train_df = predicted_train_df.filter(predicted_train_df.prediction != float("nan"))
        predicted_val_df = predicted_val_df.filter(predicted_val_df.prediction != float("nan"))
        
        predicted_train_df = predicted_train_df.withColumn("prediction", F.abs(F.round(predicted_train_df["prediction"], 0)))
        predicted_val_df = predicted_val_df.withColumn("prediction", F.abs(F.round(predicted_val_df["prediction"], 0)))
        
        training_metric = reg_eval.evaluate(predicted_train_df)
        validation_metric = reg_eval.evaluate(predicted_val_df)
 
        mlflow.log_metric("train_" + reg_eval.getMetricName(), training_metric)
        mlflow.log_metric("test_" + reg_eval.getMetricName(), validation_metric)
    
    return model, validation_metric

# COMMAND ----------

from hyperopt import fmin, tpe, hp, Trials, STATUS_OK
 
def train_with_hyperopt(params):
    
    regParam = float(params["regParam"])
    rank = int(params["rank"])
    
    model, rmse = train_als(regParam, rank)
    return {"loss": rmse, "status": STATUS_OK}

# COMMAND ----------

import numpy as np 
space = {
    "regParam": hp.uniform('regParam', 0.4, 0.8),
    "rank": hp.uniformint('rank', 4, 32)
}

# COMMAND ----------

algo=tpe.suggest
 
input_schema = Schema([
    ColSpec("integer", "walletId"),
    ColSpec("integer", "tokenId"),
])
 
output_schema = Schema([ColSpec("double")])
 
signature = ModelSignature(inputs=input_schema, outputs=output_schema)
 
modelName = "G02_model"
 
with mlflow.start_run(run_name="G02_experiment"):
    best_params=fmin(
        fn=train_with_hyperopt,
        space=space,
        algo=algo,
        max_evals=5
    )
    
    best_regParam = best_params["regParam"]
    best_rank = best_params["rank"]
 
    final_model, final_mse = train_als(best_regParam, best_rank)
    mlflow.spark.log_model(spark_model=final_model, signature=signature, artifact_path="G02-als-model", registered_model_name=modelName)

# COMMAND ----------

client = MlflowClient()
model_versions = []
# Transition this model to staging and archive the current staging model if there is one
for mv in client.search_model_versions(f"name='{modelName}'"):
    model_versions.append(dict(mv)['version'])
    if dict(mv)['current_stage'] == 'Staging':
        print("Archiving: {}".format(dict(mv)))
        # Archive the currently staged model
        client.transition_model_version_stage(
            name=modelName,
            version=dict(mv)['version'],
            stage="Archived"
        )
    
client.transition_model_version_stage(
    name=modelName,
    version=model_versions[0],  # this model (current build)
    stage="Staging"
)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
