# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount(Montar) Azure Data Lake Storage mediante Service Principal
# MAGIC 1. Obtener el valor client_id, tenant_id y client_secret del key Vault
# MAGIC 2. Configurar Spark con APP/Client Id, Directory/Tenand Id & Secret
# MAGIC 3. Utilizar el metodo "mount" de "utility" para montar el almacenamiento
# MAGIC 4. Explorar otras utilidades del sistema de archivos relacionados con el montaje (list all mounts, unmounts)

# COMMAND ----------

client_id = dbutils.secrets.get(scope="mvoie-history-secret-scope", key="movie-client-id")
tenant_id = dbutils.secrets.get(scope="mvoie-history-secret-scope", key="movie-tenant-id")
client_secret = dbutils.secrets.get(scope="mvoie-history-secret-scope", key="movie-client-secret")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@historialpeliculas.dfs.core.windows.net/",
  mount_point = "/mnt/historialpeliculas/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/historialpeliculas/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/historialpeliculas/demo/movie.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/historialpeliculas/demo")