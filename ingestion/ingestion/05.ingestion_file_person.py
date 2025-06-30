# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo "person.json"

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

from pyspark.sql.functions import current_timestamp,lit,col,concat
from pyspark.sql.types import StructType,StructField,IntegerType,StringType


# COMMAND ----------

name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

persons_schema = StructType([
    StructField("personId", IntegerType(), False),
    StructField("personName", name_schema)
    ])


# COMMAND ----------

persons_df = spark.read.schema(persons_schema).json(f"{bronze_folder_path}/person.json")

# COMMAND ----------

persons_df.printSchema()

# COMMAND ----------

display(persons_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "personid" renombrar a "person_id"
# MAGIC 2. Agregar las columnas "ingestion_date" y "enviroment"
# MAGIC 3. Agregar la columna "name" a partir de la concatenacion de "forename" y "surname"

# COMMAND ----------

persons_with_columns_df = add_ingestion_date(persons_df).withColumnRenamed("personId","person_id").withColumns({"enviroment":lit(v_enviroment)}).withColumn("name",concat(col("personName.forename"),lit(" "),col("personName.surname")))

# COMMAND ----------

display(persons_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Eliminar columnas no "requeridas"

# COMMAND ----------

persons_final_df = persons_with_columns_df.drop(col("personName"))

# COMMAND ----------

display(persons_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un formato "Parquet"

# COMMAND ----------

persons_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/persons/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/historialpeliculas/silver/persons/

# COMMAND ----------

display(spark.read.parquet(f"{silver_folder_path}/persons/"))

# COMMAND ----------

dbutils.notebook.exit("Success")