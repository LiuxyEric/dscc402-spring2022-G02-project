# Databricks notebook source
# MAGIC %md
# MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
# MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
# MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
# MAGIC - **Receipts** - the cost of gas for specific transactions.
# MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
# MAGIC - **Tokens** - Token data including contract address and symbol information.
# MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
# MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
# MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
# MAGIC 
# MAGIC In Addition, there is a price feed that changes daily (noon) that is in the **token_prices_usd** table
# MAGIC 
# MAGIC ### Rubric for this module
# MAGIC - Transform the needed information in ethereumetl database into the silver delta table needed by your modeling module
# MAGIC - Clearly document using the notation from [lecture](https://learn-us-east-1-prod-fleet02-xythos.content.blackboardcdn.com/5fdd9eaf5f408/8720758?X-Blackboard-Expiration=1650142800000&X-Blackboard-Signature=h%2FZwerNOQMWwPxvtdvr%2FmnTtTlgRvYSRhrDqlEhPS1w%3D&X-Blackboard-Client-Id=152571&response-cache-control=private%2C%20max-age%3D21600&response-content-disposition=inline%3B%20filename%2A%3DUTF-8%27%27Delta%2520Lake%2520Hands%2520On%2520-%2520Introduction%2520Lecture%25204.pdf&response-content-type=application%2Fpdf&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAAaCXVzLWVhc3QtMSJHMEUCIQDEC48E90xPbpKjvru3nmnTlrRjfSYLpm0weWYSe6yIwwIgJb5RG3yM29XgiM%2BP1fKh%2Bi88nvYD9kJNoBNtbPHvNfAqgwQIqP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARACGgw2MzU1Njc5MjQxODMiDM%2BMXZJ%2BnzG25TzIYCrXAznC%2BAwJP2ee6jaZYITTq07VKW61Y%2Fn10a6V%2FntRiWEXW7LLNftH37h8L5XBsIueV4F4AhT%2Fv6FVmxJwf0KUAJ6Z1LTVpQ0HSIbvwsLHm6Ld8kB6nCm4Ea9hveD9FuaZrgqWEyJgSX7O0bKIof%2FPihEy5xp3D329FR3tpue8vfPfHId2WFTiESp0z0XCu6alefOcw5rxYtI%2Bz9s%2FaJ9OI%2BCVVQ6oBS8tqW7bA7hVbe2ILu0HLJXA2rUSJ0lCF%2B052ScNT7zSV%2FB3s%2FViRS2l1OuThecnoaAJzATzBsm7SpEKmbuJkTLNRN0JD4Y8YrzrB8Ezn%2F5elllu5OsAlg4JasuAh5pPq42BY7VSKL9VK6PxHZ5%2BPQjcoW0ccBdR%2Bvhva13cdFDzW193jAaE1fzn61KW7dKdjva%2BtFYUy6vGlvY4XwTlrUbtSoGE3Gr9cdyCnWM5RMoU0NSqwkucNS%2F6RHZzGlItKf0iPIZXT3zWdUZxhcGuX%2FIIA3DR72srAJznDKj%2FINdUZ2s8p2N2u8UMGW7PiwamRKHtE1q7KDKj0RZfHsIwRCr4ZCIGASw3iQ%2FDuGrHapdJizHvrFMvjbT4ilCquhz4FnS5oSVqpr0TZvDvlGgUGdUI4DCdvOuSBjqlAVCEvFuQakCILbJ6w8WStnBx1BDSsbowIYaGgH0RGc%2B1ukFS4op7aqVyLdK5m6ywLfoFGwtYa5G1P6f3wvVEJO3vyUV16m0QjrFSdaD3Pd49H2yB4SFVu9fgHpdarvXm06kgvX10IfwxTfmYn%2FhTMus0bpXRAswklk2fxJeWNlQF%2FqxEmgQ6j4X6Q8blSAnUD1E8h%2FBMeSz%2F5ycm7aZnkN6h0xkkqQ%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220416T150000Z&X-Amz-SignedHeaders=host&X-Amz-Expires=21600&X-Amz-Credential=ASIAZH6WM4PLXLBTPKO4%2F20220416%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=321103582bd509ccadb1ed33d679da5ca312f19bcf887b7d63fbbb03babae64c) how your pipeline is structured.
# MAGIC - Your pipeline should be immutable
# MAGIC - Use the starting date widget to limit how much of the historic data in ethereumetl database that your pipeline processes.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)
spark.conf.set('wallet.address',wallet_address)
spark.conf.set('start.date',start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## YOUR SOLUTION STARTS HERE...

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![](https://i.imgur.com/ARFu4aO.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the Start Date from widget to find out the start block of the data.

# COMMAND ----------

partition_info = spark.sql("select start_block, end_block, min(timestamp) as start_time, max(timestamp) as end_time from ethereumetl.blocks group by start_block, end_block").toPandas()

# COMMAND ----------

from datetime import datetime
def convert_tiemstamp(x):
    return datetime.fromtimestamp(x).strftime("%Y-%m-%d")
partition_info.start_time = partition_info.start_time.apply(convert_tiemstamp)
partition_info.end_time = partition_info.end_time.apply(convert_tiemstamp)

# COMMAND ----------

partition_info.sort_values("start_block", inplace=True)

# COMMAND ----------

start_block = partition_info[partition_info.end_time >= start_date].iloc[0].start_block

# COMMAND ----------

start_block

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Schema Validation and Migration
# MAGIC In the below, we validate the schema of all the source tables that we will use. when the schema doesn't match with the predefined schema, we only retrieve the columns that we need.

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string
 
silver_contract_schema = "address STRING, bytecode STRING, is_erc20 STRING, is_erc721 STRING"
token_prices_usd_schema = "id STRING, symbol STRING, name STRING, asset_platform_id STRING, description STRING, links STRING, image STRING, contract_address STRING, sentiment_votes_up_percentage DOUBLE, sentiment_votes_down_percentage DOUBLE, market_cap_rank DOUBLE, coingecko_rank DOUBLE, coingecko_score DOUBLE, developer_score DOUBLE, community_score DOUBLE, liquidity_score DOUBLE, public_interest_score DOUBLE, price_usd DOUBLE"
tokens_schema = "address STRING, symbol STRING, name STRING, decimals BIGINT, total_supply DECIMAL(38, 0), start_block BIGINT, end_block BIGINT"
token_transfers_schema = "token_address STRING, from_address STRING, to_address STRING, value DECIMAL(38, 0), transaction_hash STRING, log_index BIGINT, block_number BIGINT, start_block BIGINT, end_block BIGINT"
 
silver_contract_df = spark.sql("select * from ethereumetl.silver_contracts")
assert silver_contract_df.schema == _parse_datatype_string(silver_contract_schema), "silver_contract schema does not match"
 
token_prices_usd_df = spark.sql("select * from ethereumetl.token_prices_usd")
assert token_prices_usd_df.schema == _parse_datatype_string(token_prices_usd_schema), "token_prices_usd_schema does not match"
 
tokens_df = spark.sql("select * from ethereumetl.tokens")
assert tokens_df.schema == _parse_datatype_string(tokens_schema), "tokens schema does not match"
 
token_transfers_df = spark.sql("select * from ethereumetl.token_transfers")
assert token_transfers_df.schema == _parse_datatype_string(token_transfers_schema), "token_transfers schema does not match"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Extract tokens that follow ERC-20 stardard
# MAGIC - In the below code block, we first extract all the tokens that are ERC-20 standard and saved it into the table g02_db.valid_token_meta_silver.
# MAGIC - we join the token's description, link and image information from ethereumetl.token_prices_usd table.
# MAGIC - The valid_token_meta_silver table is a pretty small table, no need to use optimize.

# COMMAND ----------

valid_token_meta = spark.sql(
"""
select 
  a.address as contract_address, b.name, b.symbol, c.description, c.links, c.image
from 
( select 
    distinct(address) 
  from 
    ethereumetl.silver_contracts 
  where is_erc20 = 'True') a 
join (
  select distinct name, address, symbol from ethereumetl.tokens 
) b
on a.address == b.address
join ( 
  select 
    distinct contract_address, description, links, image
  from 
    ethereumetl.token_prices_usd ) c
on b.address == c.contract_address 
order by a.address
"""
)
 
# In case we need to rerun the code, we need to first drop the table if it's already exists in the database.
spark.sql("drop table if exists g02_db.valid_token_meta_silver")
valid_token_meta.write.saveAsTable("g02_db.valid_token_meta_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe ethereumetl.token_transfers 

# COMMAND ----------

valid_token_transfer = spark.sql(
"""
select a.* from ethereumetl.token_transfers a 
join g02_db.valid_token_meta_silver b 
on a.token_address = b.contract_address
where a.start_block >= {}
""".format(start_block)
)
 
spark.sql("drop table if exists g02_db.valid_token_transfer_silver")
valid_token_transfer.write.saveAsTable("g02_db.valid_token_transfer_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from g02_db.valid_token_transfer_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize g02_db.valid_token_transfer_silver

# COMMAND ----------

triplets = spark.sql(
"""
select 
  wallet_address, 
  token_address, 
  count(*) as interacted_count 
from (
    select 
      from_address as wallet_address, 
      token_address
    from 
      g02_db.valid_token_transfer_silver
 
    union all
    
    select 
      to_address as wallet_address, 
      token_address 
    from 
      g02_db.valid_token_transfer_silver ) a
group by wallet_address, token_address
"""
)
 
spark.sql("drop table if exists g02_db.token_transfer_triplet_silver")
triplets.write.saveAsTable("g02_db.token_transfer_triplet_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize g02_db.token_transfer_triplet_silver
# MAGIC zorder by (interacted_count)

# COMMAND ----------

triplets = spark.sql("select * from g02_db.token_transfer_triplet_silver").cache()

# COMMAND ----------

unique_users = {key.wallet_address:value for value, key in enumerate(sorted(triplets.select("wallet_address").distinct().collect()))}
unique_tokens = {key.token_address:value for value, key in enumerate(sorted(triplets.select("token_address").distinct().collect()))}

# COMMAND ----------

users_ids = spark.createDataFrame(pd.DataFrame({"wallet_address":unique_users.keys(), "wallet_id":unique_users.values()}))
spark.sql("drop table if exists g02_db.wallet_address_ids_silver")
users_ids.write.saveAsTable("g02_db.wallet_address_ids_silver")
 
token_ids = spark.createDataFrame(pd.DataFrame({"token_address":unique_tokens.keys(), "token_id":unique_tokens.values()}))
spark.sql("drop table if exists g02_db.token_address_ids_silver")
token_ids.write.saveAsTable("g02_db.token_address_ids_silver")

# COMMAND ----------

from pyspark.sql import functions as F 
from pyspark.sql.types import IntegerType

# COMMAND ----------

def user_mapping(x):
    return unique_users[x]
 
def token_mapping(x):
    return unique_tokens[x]
 
user_mapping_udf = F.udf(user_mapping, IntegerType())
token_mapping_udf = F.udf(token_mapping, IntegerType())
 
triplets = (triplets.withColumn("walletId", user_mapping_udf(F.col("wallet_address")))
                          .withColumn("tokenId", token_mapping_udf(F.col("token_address"))))

# COMMAND ----------

spark.sql("drop table if exists g02_db.final_triplets_silver")
triplets.write.saveAsTable("g02_db.final_triplets_silver")

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
