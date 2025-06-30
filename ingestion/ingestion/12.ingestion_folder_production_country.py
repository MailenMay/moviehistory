# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion de la carpeta "production_country"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Leer los archivos JSON usando "DataFrameReader" de Spark

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

productions_countries_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("countryId", IntegerType(), True)
    ])


# COMMAND ----------

productions_countries_df = spark.read.schema(productions_countries_schema).option("multiLine", True).json(f"{bronze_folder_path}/production_country/")#Activo con el option la lectura multilinea del json

# COMMAND ----------

productions_countries_df.printSchema()

# COMMAND ----------

display(productions_countries_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id"
# MAGIC 2. "countryId" renombrar a "country_id"
# MAGIC 3. Agregar las columnas "ingestion_date" y "enviroment"

# COMMAND ----------

productions_countries_final_df = add_ingestion_date(productions_countries_df).withColumnsRenamed({"movieId":"movie_id","countryId":"country_id"}).withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

display(productions_countries_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato "Parquet"

# COMMAND ----------

productions_countries_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/productions_countries/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/productions_countries/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/productions_countries/"))

# COMMAND ----------

dbutils.notebook.exit("Success")