## Real Time Streaming Pipeline with Azure Databricks and Delta Live Tables  

### Introduction

Here, implement real time streaming data processing by incremental or delta load ETL pipeline by using ADF. This ETL pipeline driven by metadata tables stored on delta lake, and implemented logging, email notification via logic app after every pipeline run.

Delta Live Tables pipelines are used to define data transformations, while Delta Live Tables ensures the quality and integrity of the data throughout the process. Create Pyspark and Spark SQL code in Databricks notebook and execute through ETL pipeline. The final result is curated data on Gold layer, it will use for reporting purpose.

### Architecture

<img width="910" alt="architecture" src="https://github.com/laijupjoy/Real-Time-Streaming-Pipeline-with-Azure-Databricks-and-Delta-Live-Tables/assets/87544051/2d878821-2661-4e0d-89bc-123cc28eca85">

### ADF Copy Activity Pipeline

<img width="798" alt="ADF pipeline" src="https://github.com/laijupjoy/Real-Time-Streaming-Pipeline-with-Azure-Databricks-and-Delta-Live-Tables/assets/87544051/00f98a47-3189-4659-a05a-8f7ddd972a74">

