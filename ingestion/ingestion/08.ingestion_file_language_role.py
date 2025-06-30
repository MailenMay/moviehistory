# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo "language_role.json"

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

languages_roles_schema = StructType([
    StructField("roleId", IntegerType(), True),
    StructField("languageRole", StringType(), True)
    ])


# COMMAND ----------

languages_roles_df = spark.read.schema(languages_roles_schema).option("multiLine", True).json(f"{bronze_folder_path}/language_role.json")#Activo con el option la lectura multilinea del json

# COMMAND ----------

languages_roles_df.printSchema()

# COMMAND ----------

display(languages_roles_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "roleId" renombrar a "role_id"
# MAGIC 2. "languageRole" renombrar a "language_role"
# MAGIC 3. Agregar las columnas "ingestion_date" y "enviroment"

# COMMAND ----------

languages_roles_final_df = add_ingestion_date(languages_roles_df).withColumnsRenamed({"roleId":"role_id","languageRole":"language_role"}).withColumns({"enviroment":lit(v_enviroment)})

# COMMAND ----------

display(languages_roles_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en formato "Parquet"

# COMMAND ----------

languages_roles_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/languages_roles/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/languages_roles/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/languages_roles/"))

# COMMAND ----------

dbutils.notebook.exit("Success")