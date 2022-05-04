-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
-- MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
-- MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
-- MAGIC - **Receipts** - the cost of gas for specific transactions.
-- MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
-- MAGIC - **Tokens** - Token data including contract address and symbol information.
-- MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
-- MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
-- MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
-- MAGIC 
-- MAGIC ### Rubric for this module
-- MAGIC Answer the quetions listed below.

-- COMMAND ----------

-- MAGIC %run ./includes/utilities

-- COMMAND ----------

-- MAGIC %run ./includes/configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Grab the global variables
-- MAGIC wallet_address,start_date = Utils.create_widgets()
-- MAGIC print(wallet_address,start_date)
-- MAGIC spark.conf.set('wallet.address',wallet_address)
-- MAGIC spark.conf.set('start.date',start_date)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the maximum block number and date of block in the database

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select a.number as max_block_number, from_unixtime(timestamp) as date
-- MAGIC from ethereumetl.blocks a
-- MAGIC join (select max(number) as max_number from ethereumetl.blocks) b
-- MAGIC on a.number = b.max_number;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: At what block did the first ERC20 transfer happen?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct number, from_unixtime(timestamp) date
-- MAGIC from ethereumetl.blocks
-- MAGIC where from_unixtime(timestamp) = (select min(from_unixtime(timestamp)) as first_transfer from ethereumetl.token_transfers
-- MAGIC left join ethereumetl.blocks
-- MAGIC on number = block_number);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: How many ERC20 compatible contracts are there on the blockchain?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) erc20_contracts
-- MAGIC from ethereumetl.silver_contracts
-- MAGIC where is_erc20 = True;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Q: What percentage of transactions are calls to contracts

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select ((select count(*) from ethereumetl.transactions where to_address = '')/count(*)) trans_to_contracts
-- MAGIC from ethereumetl.transactions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What are the top 100 tokens based on transfer count?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select name, count(*) transfers
-- MAGIC from ethereumetl.tokens a
-- MAGIC right join ethereumetl.token_transfers b
-- MAGIC on a.address = b.token_address
-- MAGIC group by name
-- MAGIC order by count(*) desc
-- MAGIC limit 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What fraction of ERC-20 transfers are sent to new addresses
-- MAGIC (i.e. addresses that have a transfer count of 1 meaning there are no other transfers to this address for this token this is the first)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select (select count(*)
-- MAGIC from (select count(*) new from ethereumetl.token_transfers
-- MAGIC group by to_address
-- MAGIC having new = 1))/count(*) fraction_to_new from ethereumetl.token_transfers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: In what order are transactions included in a block in relation to their gas price?
-- MAGIC - hint: find a block with multiple transactions 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select block_number, gas_price, transaction_index
-- MAGIC from ethereumetl.transactions
-- MAGIC where block_number = (select block_number from ethereumetl.transactions
-- MAGIC group by block_number
-- MAGIC order by count(*) desc
-- MAGIC limit 1)
-- MAGIC order by gas_price;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What was the highest transaction throughput in transactions per second?
-- MAGIC hint: assume 15 second block time

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select max(value/power(10,18)/15) max_trans_per_sec from ethereumetl.transactions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total Ether volume?
-- MAGIC Note: 1x10^18 wei to 1 eth and value in the transaction table is in wei

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select sum(value)/power(10,18) ether_volume from ethereumetl.transactions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total gas used in all transactions?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select sum(gas_used) total_gas from ethereumetl.blocks;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Maximum ERC-20 transfers in a single transaction

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select max(n) max_transfers
-- MAGIC from (select transaction_index, sum(value) n
-- MAGIC from ethereumetl.transactions
-- MAGIC group by transaction_index) c;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Token balance for any address on any date?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select address, sum(total_supply) token_balance
-- MAGIC from ethereumetl.tokens
-- MAGIC group by address;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz the transaction count over time (network use)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select cast(from_unixtime(timestamp) as date) as date, count(*) transaction_count
-- MAGIC from ethereumetl.blocks a
-- MAGIC right join ethereumetl.transactions b
-- MAGIC on a.number = b.block_number
-- MAGIC group by date
-- MAGIC order by date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz ERC-20 transfer count over time
-- MAGIC interesting note: https://blog.ins.world/insp-ins-promo-token-mixup-clarified-d67ef20876a3

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select cast(from_unixtime(timestamp) as date) as date, count(*) erc20_transfer_count
-- MAGIC from ethereumetl.blocks a
-- MAGIC right join ethereumetl.token_transfers b
-- MAGIC on a.number = b.block_number
-- MAGIC group by date
-- MAGIC order by date;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Return Success
-- MAGIC dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
