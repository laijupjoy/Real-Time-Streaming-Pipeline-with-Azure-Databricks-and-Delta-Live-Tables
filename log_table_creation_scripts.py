# Databricks notebook source
dbutils.widgets.text("storage_account_name", "")
storage_account_name = dbutils.widgets.get("storage_account_name")
dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

# COMMAND ----------

spark.sql(f"""
create database if not exists hive_metastore.{env}_log       
""")

# COMMAND ----------

spark.sql(f"""     
create or replace table {env}_log.log_record_tbl (
    env string,
    pipeLineName string,
    logMessage string,
    status string,
    triggerType string,
    loadId string,
    logTimeStamp timestamp
)      
location 'abfss://log@{storage_account_name}.dfs.core.windows.net/log_record_tbl'    
""")
