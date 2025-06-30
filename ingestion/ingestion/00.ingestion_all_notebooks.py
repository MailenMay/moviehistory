# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("01.ingestion_file_movie", 0,{"p_enviroment":"developer"})#0 tiempo en segundos, el diccionario es para los parametros que tenga en mi notebook. Con la asignacion a la variable capturo el msj que ofrece el notebook.

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("02.ingestion_file_language", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("03.ingestion_file_genre", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("04.ingestion_file_country", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("05.ingestion_file_person", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("06.ingestion_file_movie_genre", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("07.ingestion_file_movie_cast", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("08.ingestion_file_language_role", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("09.ingestion_folder_production_company", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("10.ingestion_folder_movie_company", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("11.ingestion_folder_movie_language", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("12.ingestion_folder_production_country", 0,{"p_enviroment":"developer"})

# COMMAND ----------

v_result