# Databricks notebook source
dbutils.widgets.text("storage_account_name", "")
storage_account_name = dbutils.widgets.get("storage_account_name")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists hive_metastore.metadata_schema

# COMMAND ----------

#source_param_tbl metadata table holds the parameters values for various connections
spark.sql(f"""
create or replace table metadata_schema.parameters_tbl (
    source_name string,
    host_name string,
    port int,
    source_db_name string,
    source_user_name string,
    secret_name string,
    adls_storage_account_name string,
    adls_url string,
    adls_container_name string,
    logic_app_url string,
    email_id string,
    job_id string
)
location 'abfss://metadata@{storage_account_name}.dfs.core.windows.net/parameters_tbl'
""")

# COMMAND ----------

spark.sql(f"""
insert overwrite metadata_schema.parameters_tbl values 
('mysql','localhost',3306,'fmcg','root','mysql-password','csadlsgen2storageacc24','https://csadlsgen2storageacc24.dfs.core.windows.net/','landing','NA','NA',111),
('mysql','localhost',3306,'finance','root','mysql-password','csadlsgen2storageacc24','https://csadlsgen2storageacc24.dfs.core.windows.net/','landing','NA','NA',222)          
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata_schema.parameters_tbl
