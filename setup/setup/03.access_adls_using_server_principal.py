# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceder a Azure Data Lake Storage mediante Service Principal
# MAGIC 1. "Registrar la aplicacion" en Azure Entra ID / Service Principal
# MAGIC 2. Generar un secreto(Contraseña) para la aplicacion
# MAGIC 3. Configurar Spark con APP / Client Id, Directory / Tenand Id & Secret
# MAGIC 4. Asignar el Role "Storage Blob Data Contributor" al Data Lake.

# COMMAND ----------

client_id = dbutils.secrets.get(scope="mvoie-history-secret-scope", key="movie-client-id")
tenant_id = dbutils.secrets.get(scope="mvoie-history-secret-scope", key="movie-tenant-id")
client_secret = dbutils.secrets.get(scope="mvoie-history-secret-scope", key="movie-client-secret")


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.historialpeliculas.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.historialpeliculas.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.historialpeliculas.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.historialpeliculas.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.historialpeliculas.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@historialpeliculas.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@historialpeliculas.dfs.core.windows.net/movie.csv"))