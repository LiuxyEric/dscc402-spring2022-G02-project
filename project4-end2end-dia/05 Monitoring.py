# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Implement a routine to "promote" your model at **Staging** in the registry to **Production** based on a boolean flag that you set in the code.
# MAGIC - Using wallet addresses from your **Staging** and **Production** model test data, compare the recommendations of the two models.

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
# MAGIC ## Your Code Starts Here...

# COMMAND ----------

# necessary packages
from pyspark.sql.types import *
import mlflow 
import mlflow.spark 
from mlflow.tracking import MlflowClient 
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F 

# COMMAND ----------

# MAGIC %md
# MAGIC * In order to promote the Staging model into Production, we need to define a rmse threshold. 
# MAGIC * When the Staging model's rmse in test data is below the threshold, we can promote our model into production.

# COMMAND ----------

rmse_threshold = 3500

# COMMAND ----------

# load testing data 

triplets = spark.sql("select * from g02_db.final_triplets_silver").cache()

seed = 42

(split_60_df, split_a_20_df, split_b_20_df) = triplets.randomSplit([0.6, 0.2, 0.2], seed=seed)
test_df = split_b_20_df.cache()
test_df = test_df.withColumn("interacted_count", test_df["interacted_count"].cast(DoubleType())).cache()
reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="interacted_count", metricName="rmse")

# COMMAND ----------

def model_performance(modelName, stage):
    model = mlflow.spark.load_model("models:/" + modelName + "/{}".format(stage))
    predicted_test_df = model.transform(test_df)
    predicted_test_df = predicted_test_df.filter(predicted_test_df.prediction != float("nan"))
    predicted_test_df = predicted_test_df.withColumn("prediction", F.abs(F.round(predicted_test_df["prediction"], 0)))
    test_metric = reg_eval.evaluate(predicted_test_df)
    return test_metric

# COMMAND ----------

modelName = "G02_model"
client = MlflowClient()
model_versions = []
# Transition this model to staging and archive the current staging model if there is one
for mv in client.search_model_versions(f"name='{modelName}'"):
    print(dict(mv)['version'])

# COMMAND ----------

modelName = "G02_model"
staging_model_performace = model_performance(modelName, "Staging")

if staging_model_performace < rmse_threshold:
    client = MlflowClient()
    model_versions = []
    # Transition this model to staging and archive the current staging model if there is one
    for mv in client.search_model_versions(f"name='{modelName}'"):
        model_versions.append(dict(mv)['version'])
        
    client.transition_model_version_stage(
        name=modelName,
        version=model_versions[0],  # this model (current build)
        stage="Production"
    )

# COMMAND ----------

print("Performace of staging model: {}, \nPerformance of production model: {}".format(staging_model_performace, model_performance(modelName, "Production")))

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
