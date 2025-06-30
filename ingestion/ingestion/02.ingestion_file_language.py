# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "language.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Leer el archivo CSV usando "DataFrameReader" de Spark

# COMMAND ----------

dbutils.widgets.text("p_enviroment", "")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import current_timestamp,lit,col 


# COMMAND ----------

language_schema = StructType(fields=[
    StructField("languageId", IntegerType(), False),
    StructField("languageCode", StringType(), True),
    StructField("languageName", StringType(), True)
])

# COMMAND ----------

language_df = spark.read. \
    option("header", True) \
    .schema(language_schema) \
    .csv(f"{bronze_folder_path}/language.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Seleccionar solo las columnas "requeridas"

# COMMAND ----------

language_selected_df = language_df.select(col("languageId"), col("languageName"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Cambiar el nombre a las columnas segun lo "requerido"

# COMMAND ----------

language_renamed_df = language_selected_df \
                    .withColumnsRenamed({"languageId": "language_id", "languageName": "language_name"})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Agregar una columna "ingestion_date" y "enviroment" al DataFrame

# COMMAND ----------

language_final_df = add_ingestion_date(language_renamed_df).withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 5 - Escribir datos en el datalake en formato "Parquet"

# COMMAND ----------

language_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/languages/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/languages/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/languages/"))

# COMMAND ----------

dbutils.notebook.exit("Success")