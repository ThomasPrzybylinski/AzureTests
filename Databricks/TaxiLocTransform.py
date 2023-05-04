# Databricks notebook source
service_credential = dbutils.secrets.get(scope="datalake",key="datalakeeastkey")

spark.conf.set("fs.azure.account.auth.type.tprzybyldatalakeeast.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.tprzybyldatalakeeast.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.tprzybyldatalakeeast.dfs.core.windows.net", service_credential)

dbutils.fs.ls("abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/")

datalakeroot="abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/"

# COMMAND ----------

df = spark.read.format('csv').option("header","true").load(datalakeroot+'Landing/taxi+_zone_lookup.csv')

# COMMAND ----------

df_transformed = df.withColumnRenamed("service_zone","ServiceZone")
df_transformed = df.withColumn("LocationID",df.LocationID.cast("long"))

# COMMAND ----------

df_transformed.write.format('parquet').mode("overwrite").save(datalakeroot+'Clean/Location')

# COMMAND ----------

df_transformed.show()

# COMMAND ----------

df_transformed.printSchema()

# COMMAND ----------


