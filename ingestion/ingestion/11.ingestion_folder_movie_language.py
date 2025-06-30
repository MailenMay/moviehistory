# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion de la carpeta "movie_language"

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

movies_languages_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("languageId", IntegerType(), True),
    StructField("languageRoleId", IntegerType(), True)
    ])


# COMMAND ----------

movies_languages_df = spark.read.schema(movies_languages_schema).option("multiLine", True).json(f"{bronze_folder_path}/movie_language/")#Activo con el option la lectura multilinea del json

# COMMAND ----------

movies_languages_df.printSchema()

# COMMAND ----------

display(movies_languages_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id"
# MAGIC 2. "languageId" renombrar a "language_id"
# MAGIC 3. Agregar las columnas "ingestion_date" y "enviroment"

# COMMAND ----------

movies_languages_with_columns_df = add_ingestion_date(movies_languages_df).withColumnsRenamed({"movieId":"movie_id","languageId":"language_id"}).withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

display(movies_languages_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Eliminar la columna "languageRoleId"

# COMMAND ----------

movies_languages_final_df = movies_languages_with_columns_df.drop(col("languageRoleId"))

# COMMAND ----------

display(movies_languages_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en formato "Parquet"

# COMMAND ----------

movies_languages_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/movies_languages/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/movies_languages/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/movies_languages/"))

# COMMAND ----------

dbutils.notebook.exit("Success")