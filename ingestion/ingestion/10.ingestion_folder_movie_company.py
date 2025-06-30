# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion de la carpeta "movie_company"

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
from pyspark.sql.types import StructType,StructField,IntegerType


# COMMAND ----------

movies_companies_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("companyId", IntegerType(), True)
    ])


# COMMAND ----------

movies_companies_df = spark.read.schema(movies_companies_schema).csv(f"{bronze_folder_path}/movie_company/")#Si tengo mas archivos en la carpeta y solo quiero cargar los del movie_company deberia usar movie_company_*.csv

# COMMAND ----------

movies_companies_df.printSchema()

# COMMAND ----------

display(movies_companies_df)

# COMMAND ----------

movies_companies_df.count()#Cuento los registros

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id"
# MAGIC 2. "companyId" renombrar a "company_id"
# MAGIC 3. Agregar las columnas "ingestion_date" y "enviroment"

# COMMAND ----------

movies_companies_final_df = add_ingestion_date(movies_companies_df).withColumnsRenamed({"movieId":"movie_id","companyId":"company_id"}).withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

display(movies_companies_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en formato "Parquet"

# COMMAND ----------

movies_companies_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/movies_companies/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/movies_companies/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/movies_companies/"))

# COMMAND ----------

dbutils.notebook.exit("Success")