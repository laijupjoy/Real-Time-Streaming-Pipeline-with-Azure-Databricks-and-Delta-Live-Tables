# Databricks notebook source
#creating widgets to get the parameters
dbutils.widgets.text("env","")
dbutils.widgets.text("pipeLineName","")
dbutils.widgets.text("logMessage","")
dbutils.widgets.text("status","")
dbutils.widgets.text("triggerType","")
dbutils.widgets.text("loadId","")
dbutils.widgets.text("logTimeStamp","")

# COMMAND ----------

#Creating Variables for Wedgts
env=dbutils.widgets.get("env")
pipeLineName=dbutils.widgets.get("pipeLineName")
logMessage=dbutils.widgets.get("logMessage")
status=dbutils.widgets.get("status")
triggerType=dbutils.widgets.get("triggerType")
loadId=dbutils.widgets.get("loadId")
logTimeStamp=dbutils.widgets.get("logTimeStamp")

# COMMAND ----------

#To escape single quotes, use back slash(\)
status = status.replace('\'','"')

# COMMAND ----------

#Updating the pipeline run status to log table
spark.sql(f"""insert into {env}_log.log_record_tbl values ('{env}','{pipeLineName}','{logMessage}','{status}','{triggerType}','{loadId}',cast('{logTimeStamp}' as Timestamp))""")
