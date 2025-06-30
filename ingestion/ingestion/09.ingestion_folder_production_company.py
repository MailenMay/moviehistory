# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion de la carpeta "production_company"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Leer los archivos CSV usando "DataFrameReader" de Spark

# COMMAND ----------

dbutils.widgets.text("p_enviroment", "")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col
from pyspark.sql.types import StructType,StructField,IntegerType,StringType


# COMMAND ----------

productions_companies_schema = StructType([
    StructField("companyId", IntegerType(), True),
    StructField("companyName", StringType(), True)
    ])


# COMMAND ----------

productions_companies_df = spark.read.schema(productions_companies_schema).csv(f"{bronze_folder_path}/production_company/")#Si tengo mas archivos en la carpeta y solo quiero cargar los del production_company deberia usar production_company_*.csv

# COMMAND ----------

productions_companies_df.printSchema()

# COMMAND ----------

display(productions_companies_df)

# COMMAND ----------

productions_companies_df.count()#Cuento los registros

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "companyId" renombrar a "company_id"
# MAGIC 2. "companyName" renombrar a "company_name"
# MAGIC 3. Agregar las columnas "ingestion_date" y "enviroment"

# COMMAND ----------

productions_companies_final_df = add_ingestion_date(productions_companies_df).withColumnsRenamed({"companyId":"company_id","companyName":"company_name"}).withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

display(productions_companies_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en formato "Parquet"

# COMMAND ----------

productions_companies_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/productions_companies/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/productions_companies/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/productions_companies/"))

# COMMAND ----------

dbutils.notebook.exit("Success")