# Databricks notebook source
# MAGIC %md
# MAGIC ### Explorar las capacidades de la utilidad "dbutils.secrets"

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="mvoie-history-secret-scope")

# COMMAND ----------

dbutils.secrets.get(scope="mvoie-history-secret-scope", key="movie-access-key")