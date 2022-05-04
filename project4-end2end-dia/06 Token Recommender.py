# Databricks notebook source
# MAGIC %md
# MAGIC ## Token Recommendation
# MAGIC <table border=0>
# MAGIC   <tr><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/rec-application.png'></td>
# MAGIC     <td>Your application should allow a specific wallet address to be entered via a widget in your application notebook.  Each time a new wallet address is entered, a new recommendation of the top tokens for consideration should be made. <br> **Bonus** (3 points): include links to the Token contract on the blockchain or etherscan.io for further investigation.</td></tr>
# MAGIC   </table>

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
# MAGIC ## Your code starts here...

# COMMAND ----------

from pyspark.sql import DataFrame 
from pyspark.sql.types import * 
from pyspark.sql import functions as F 
from delta.tables import * 
import random 

import mlflow 
import mlflow.spark 
from mlflow.tracking import MlflowClient 
from mlflow.models.signature import infer_signature, ModelSignature
from mlflow.types.schema import Schema, ColSpec

from pyspark.ml.recommendation import ALS 
from pyspark.ml.evaluation import RegressionEvaluator 

from PIL import Image
import requests
from io import BytesIO 
import base64
from IPython.display import HTML 

# COMMAND ----------

def image_base64(im):
    response = requests.get(im)
    im = Image.open(BytesIO(response.content))
    with BytesIO() as buffer:
        if im.mode in ("RGBA", "P"):
            im = im.convert("RGB")
        im.save(buffer, "JPEG")
        return base64.b64encode(buffer.getvalue()).decode()

def image_formatter(im):
    return f'<img src="data:image/jpeg;base64,{image_base64(im)}">'

def superlink_formatter(link):
    return f'<a href="{link}"><img src="https://img.icons8.com/flat-round/32/000000/link--v1.png"/>'

# COMMAND ----------

class TokenRecommender:
    def __init__(self, dataName, metaName, modelName):
        
        self.dataName = dataName
        self.modelName = modelName 
        self.model = None 
        
        self.triplets = spark.sql("select * from g02_db.{}".format(dataName)).cache()
        self.metadata = spark.sql("select * from g02_db.{}".format(metaName)).cache()
        
        self.unique_users = spark.sql("select * from g02_db.wallet_address_ids_silver").toPandas()
        self.unique_users = dict(zip(self.unique_users.wallet_address, self.unique_users.wallet_id))
        
        self.unique_tokens = spark.sql("select * from g02_db.token_address_ids_silver").toPandas()
        self.unique_tokens = dict(zip(self.unique_tokens.token_address, self.unique_tokens.token_id))
    
    def load_model(self):
        
        if self.model is None:
            self.model = mlflow.spark.load_model("models:/" + self.modelName + "/Production")
    
    def recommend(self, wallet_address: str, n):
        
        walletId = self.unique_users[wallet_address]
        transferred_tokens = self.triplets.filter(self.triplets.wallet_address == wallet_address)
        
        untransferred_tokens = (self.triplets.filter(~self.triplets["tokenId"].isin([token["tokenId"] for token in transferred_tokens.collect()]))
                                .select("wallet_address", "token_address", "tokenId").withColumn("walletId", F.lit(walletId)).distinct())
        
        self.load_model()
        
        predicted_df = self.model.transform(untransferred_tokens)
        
        result = (predicted_df.join(self.metadata, predicted_df["token_address"] == self.metadata["contract_address"])
                  .select("name", "symbol", "links", "image", "prediction")
                  .distinct()
                  .orderBy("prediction", ascending=False)
                  .limit(n)
                  .toPandas())
        
        result["token_name"] = result.apply(lambda x: "{}({})".format(x["name"], x["symbol"]), axis=1)
        result = result[["image", "token_name", "links"]]
        return result

# COMMAND ----------

wallet_address = "0x000e1ccf6787e7dfdf34ffbf7f951e4981c558e0"
result = recommender.recommend(wallet_address, 10)    
print("Recommend Token For User Address: {}".format(wallet_address))
HTML(result.to_html(formatters={"image": image_formatter, "links": superlink_formatter}, escape=False, header=False, index=False, classes="table table-hover"))  

# COMMAND ----------

wallet_address = "0x000efb21637da3b79cc9c3dcec1cb52a0f28ae28"
result = recommender.recommend(wallet_address, 10)    
print("Recommend Token For User Address: {}".format(wallet_address))
HTML(result.to_html(formatters={"image": image_formatter, "links": superlink_formatter}, escape=False, header=False, index=False, classes="table table-hover"))  

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
