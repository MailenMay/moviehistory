# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo "movie_cast.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Leer el archivo JSON usando "DataFrameReader" de Spark

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

movies_casts_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("personId", IntegerType(), True),
    StructField("characterName", StringType(), True),
    StructField("genderId", IntegerType(), True),
    StructField("castOrder", IntegerType(), True)
    ])


# COMMAND ----------

movies_casts_df = spark.read.schema(movies_casts_schema).option("multiLine", True).json(f"{bronze_folder_path}/movie_cast.json")#Activo con el option la lectura multilinea del json

# COMMAND ----------

movies_casts_df.printSchema()

# COMMAND ----------

display(movies_casts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id"
# MAGIC 2. "personId" renombrar a "person_id"
# MAGIC 3. "characterName" renombrar a "character_name"
# MAGIC 4. Agregar las columnas "ingestion_date" y "enviroment"

# COMMAND ----------

movies_casts_with_columns_df = add_ingestion_date(movies_casts_df).withColumnsRenamed({"movieId":"movie_id","personId":"person_id","characterName":"character_name"}).withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

display(movies_casts_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Eliminar las columnas no deseadas del DataFrame

# COMMAND ----------

movies_casts_final_df = movies_casts_with_columns_df.drop(col("genderId"),col("castOrder"))

# COMMAND ----------

display(movies_casts_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en formato "Parquet"

# COMMAND ----------

movies_casts_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/movies_casts/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/movies_casts/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/movies_casts/"))

# COMMAND ----------

dbutils.notebook.exit("Success")