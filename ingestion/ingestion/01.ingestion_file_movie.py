# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "movie.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Leer el archivo CSV usando "DataFrameReader" de Spark

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_enviroment", "")
v_enviroment = dbutils.widgets.get("p_enviroment")


# COMMAND ----------

v_enviroment

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

bronze_folder_path

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType


# COMMAND ----------

movie_schema = StructType(fields=[
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("budget", DoubleType(), True),
    StructField("homepage", StringType(), True  ),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("yearReleaseDate", IntegerType(), True),
    StructField("releaseDate", DateType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("durationTime", IntegerType(), True),
    StructField("movieStatus", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("voteAverage", DoubleType(), True),
    StructField("voteCount", IntegerType(), True)
])#Defino estructura que tendra el df: primero nombnre campo, segundo tipo de dato, tercero si permite nulos

# COMMAND ----------

movie_df = spark.read. \
    option("header", True) \
    .schema(movie_schema) \
    .csv(f"{bronze_folder_path}/movie.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Seleccionar solo las columnas "requeridas"

# COMMAND ----------

from pyspark.sql.functions import col #Importo col para seleccionar columnas

# COMMAND ----------

movies_selected_df = movie_df.select(col("movieId"), col("title"), col("budget"), col("popularity"), col("yearReleaseDate"), col("releaseDate"), col("revenue"), col("durationTime"), col("voteAverage"), col("voteCount"))#Con las ultimas 3 funciones puedo agregar otras funciones, con la primera no.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Cambiar el nombre a las columnas segun lo "requerido"

# COMMAND ----------

movies_renamed_df = movies_selected_df \
                    .withColumnRenamed("movieId", "movie_id") \
                    .withColumnRenamed("yearReleaseDate", "year_release_date") \
                    .withColumnRenamed("releaseDate", "release_date") \
                    .withColumnRenamed("durationTime", "duration_time") \
                    .withColumnRenamed("voteAverage", "vote_average") \
                    .withColumnRenamed("voteCount", "vote_count")

# COMMAND ----------

movies_renamed_df = movies_selected_df \
                    .withColumnsRenamed({"movieId": "movie_id","yearReleaseDate": "year_release_date","releaseDate": "release_date","durationTime": "duration_time","voteAverage": "vote_average","voteCount": "vote_count"})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Agregar una columna "ingestion_date" al DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

movies_final_df = add_ingestion_date(movies_renamed_df) \
                                   .withColumn("enviroment", lit(v_enviroment))#Con lit paso el valor como objeto, porque si no no puedo usar current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 5 - Escribir datos en el datalake en formato "Parquet"

# COMMAND ----------

movies_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/movies/")#Con mode("overwrite") sobreescribo el archivo y me evito errores| Con partitionBy particiono el archivo para guardarse por el campo que indique

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/movies/

# COMMAND ----------

df = spark.read.parquet("/mnt/historialpeliculas/silver/movies/")

# COMMAND ----------

display(df)


# COMMAND ----------

display(spark.read.parquet("/mnt/historialpeliculas/silver/movies/"))

# COMMAND ----------

dbutils.notebook.exit("Success")