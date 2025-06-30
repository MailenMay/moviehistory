# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "movie_genre.json"

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


# COMMAND ----------

movie_genre_schema = "movieId INT, genreId INT"

# COMMAND ----------

movie_genre_df = spark.read.schema(movie_genre_schema).json(f"{bronze_folder_path}/movie_genre.json")

# COMMAND ----------

movie_genre_df.printSchema()

# COMMAND ----------

display(movie_genre_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Cambiar el nombre a las columnas y añadir "ingestion_date" y "enviroment"

# COMMAND ----------

movie_genre_final_df = add_ingestion_date(movie_genre_df).withColumnRenamed("movieId","movie_id").withColumnRenamed("genreId","genre_id").withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

display(movie_genre_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un formato "Parquet"

# COMMAND ----------

movie_genre_final_df.write.mode("overwrite").partitionBy("movie_id").parquet(f"{silver_folder_path}/movie_genre/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/movie_genre/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/movie_genre/"))

# COMMAND ----------

dbutils.notebook.exit("Success")