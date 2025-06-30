# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "genre.csv"

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp,lit,col 


# COMMAND ----------

genre_schema = StructType(fields=[
    StructField("genreId", IntegerType(), False),
    StructField("genreName", StringType(), True)
])

# COMMAND ----------

genre_df = spark.read. \
    option("header", True) \
    .schema(genre_schema) \
    .csv(f"{bronze_folder_path}/genre.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Cambiar el nombre a las columnas segun lo "requerido"

# COMMAND ----------

genre_renamed_df = genre_df \
                    .withColumnsRenamed({"genreId": "genre_id", "genreName": "genre_name"})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Agregar una columna "ingestion_date" y "enviroment" al DataFrame

# COMMAND ----------

genre_final_df = add_ingestion_date(genre_renamed_df).withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 5 - Escribir datos en el datalake en formato "Parquet"

# COMMAND ----------

genre_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/genres/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/genres/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/genres/"))

# COMMAND ----------

dbutils.notebook.exit("Success")