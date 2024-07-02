-- Databricks notebook source
--Ingesting data from landing container to bronze streaming table fact_transactions_bronze

CREATE OR REFRESH STREAMING TABLE fact_transactions_bronze
Location 'abfss://bronze@csadlsgen2storageacc24.dfs.core.windows.net/fact_transactions_bronze'
AS SELECT 
          customer_id,
          month,
          category,
          payment_type,
          spend,
          transaction_id,
          _rescued_data,
          current_timestamp() as load_time
FROM cloud_files('abfss://landing@csadlsgen2storageacc24.dfs.core.windows.net/kafka_consumer_sink/', 
                 "csv", map(
                  "cloudFiles.inferColumnTypes", "true")  
                  )

-- COMMAND ----------

--Ingesting data from landing container to bronze streaming table dim_transactions_bronze

CREATE
OR REFRESH STREAMING TABLE dim_transactions_bronze Location 'abfss://bronze@csadlsgen2storageacc24.dfs.core.windows.net/dim_transactions_bronze' AS
SELECT
  customer_id,
  age_group,
  city,
  occupation,
  gender,
  `marital status` as marital_status,
  avg_income,
  _rescued_data,
  current_timestamp() as load_time
FROM
  cloud_files(
    'abfss://landing@csadlsgen2storageacc24.dfs.core.windows.net/mysql/finance/',
    "csv",
    map(
      "cloudFiles.inferColumnTypes",
      "true"
    )
  )

-- COMMAND ----------

-- INSERTING RECORDS IN SILVER LAYER TABLES (DATA QUALITY CHECKS)

-- COMMAND ----------

--Ingesting data from bronze streaming table fact_transactions_bronze to silver streaming table fact_transactions_silver_temp

CREATE
OR REFRESH STREAMING TABLE fact_transactions_silver_temp (
  CONSTRAINT customer_id_exists EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
) Location 'abfss://silver@csadlsgen2storageacc24.dfs.core.windows.net/fact_transactions_silver_temp' AS
SELECT
  customer_id,
  month as transaction_month,
  category,
  payment_type,
  spend,
  transaction_id,
  load_time
FROM
  STREAM(LIVE.fact_transactions_bronze)

-- COMMAND ----------

--Ingesting data from bronze streaming table dim_transactions_bronze to silver streaming table dim_transactions_silver_temp

CREATE
OR REFRESH STREAMING TABLE dim_transactions_silver_temp (
  CONSTRAINT customer_id_exists EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
) Location 'abfss://silver@csadlsgen2storageacc24.dfs.core.windows.net/dim_transactions_silver_temp' AS
SELECT
  customer_id,
  age_group,
  city,
  occupation,
  gender,
  marital_status,
  avg_income,
  load_time
FROM
  STREAM(LIVE.dim_transactions_bronze)

-- COMMAND ----------

-- INSERTING RECORDS IN SILVER LAYER TABLE (SCD TYPE 1 AND 2 IMPLEMENTATION)

-- COMMAND ----------

--Ingesting data from fact_transactions_silver_temp to fact_transactions_silver

CREATE OR REFRESH STREAMING TABLE fact_transactions_silver
Location 'abfss://silver@csadlsgen2storageacc24.dfs.core.windows.net/fact_transactions_silver';

APPLY CHANGES INTO LIVE.fact_transactions_silver
FROM STREAM(LIVE.fact_transactions_silver_temp)
KEYS (transaction_id)
SEQUENCE BY load_time
STORED AS SCD TYPE 2;

-- COMMAND ----------

--Ingesting data from dim_transactions_silver_temp to dim_transactions_silver

CREATE OR REFRESH STREAMING TABLE dim_transactions_silver
Location 'abfss://silver@csadlsgen2storageacc24.dfs.core.windows.net/dim_transactions_silver';

APPLY CHANGES INTO LIVE.dim_transactions_silver
FROM STREAM(LIVE.dim_transactions_silver_temp)
KEYS (customer_id)
SEQUENCE BY load_time
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- INSERTING RECORDS IN Gold LAYER TABLE (Creating materialized Views)

-- COMMAND ----------

--Total Spend by Customer's Occupation and Gender

CREATE LIVE TABLE total_spend_by_occupation_gender
Location'abfss://gold@csadlsgen2storageacc24.dfs.core.windows.net/total_spend_by_occupation_gender'
SELECT
  d.occupation,
  d.gender,
  SUM(f.spend) AS total_spend
FROM
  LIVE.fact_transactions_silver f
JOIN 
  LIVE.dim_transactions_silver d 
ON 
  f.customer_id = d.customer_id
GROUP BY
  d.occupation,
  d.gender
ORDER BY
  d.occupation,
  d.gender;

-- COMMAND ----------

--Average Spend by Age Group

CREATE LIVE TABLE average_spend_by_age_group
Location'abfss://gold@csadlsgen2storageacc24.dfs.core.windows.net/average_spend_by_age_group'
SELECT 
    d.age_group,
    AVG(f.spend) AS average_spend
FROM 
    LIVE.fact_transactions_silver f
JOIN 
    LIVE.dim_transactions_silver d
ON 
    f.customer_id = d.customer_id
GROUP BY 
    d.age_group
ORDER BY 
    d.age_group

-- COMMAND ----------

--Customer Distribution by City and Transaction Category

CREATE LIVE TABLE customer_distribution_by_city_category
Location'abfss://gold@csadlsgen2storageacc24.dfs.core.windows.net/customer_distribution_by_city_category'
SELECT 
    d.city,
    f.category,
    COUNT(DISTINCT f.customer_id) AS customer_count
FROM 
    LIVE.fact_transactions_silver f
JOIN 
    LIVE.dim_transactions_silver d
ON 
    f.customer_id = d.customer_id
GROUP BY 
    d.city,
    f.category
ORDER BY 
    d.city, 
    f.category
