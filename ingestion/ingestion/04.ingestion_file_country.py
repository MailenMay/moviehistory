# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "country.json"

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

countries_schema = "countryId INT, countryIsoCode STRING, countryName STRING"

# COMMAND ----------

countries_df = spark.read.schema(countries_schema).json(f"{bronze_folder_path}/country.json")

# COMMAND ----------

countries_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Eliminar columnas no deseadas del DataFrame

# COMMAND ----------

countries_dropped_df = countries_df.drop("countryIsoCode")

# COMMAND ----------

countries_dropped_df = countries_df.drop(countries_df["countryIsoCode"])

# COMMAND ----------

countries_dropped_df = countries_df.drop(col("countryIsoCode"))

# COMMAND ----------

display(countries_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Cambiar el nombre a las columnas y añadir "ingestion_date" y "enviroment"

# COMMAND ----------

countries_final_df = add_ingestion_date(countries_dropped_df).withColumnRenamed("countryId","country_id").withColumnRenamed("countryName","country_name").withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

display(countries_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un formato "Parquet"

# COMMAND ----------

countries_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/countries/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/countries/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/countries/"))

# COMMAND ----------

dbutils.notebook.exit("Success")