# Databricks notebook source
service_credential = dbutils.secrets.get(scope="datalake",key="datalakeeastkey")

spark.conf.set("fs.azure.account.auth.type.tprzybyldatalakeeast.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.tprzybyldatalakeeast.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.tprzybyldatalakeeast.dfs.core.windows.net", service_credential)

dbutils.fs.ls("abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/")

datalakeroot="abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/"

# COMMAND ----------

df = spark.read.load('abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/Clean/TaxiTrip', format="delta")
df.show(10)
#df = spark.read.load('abfss://tprzybyldatalakeeast@datalake.dfs.core.windows.net/Landing/taxi+_zone_lookup.csv', format="csv")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *

# COMMAND ----------

df.groupBy("VendorID").agg(sum(col("FareAmount")).alias("TotalMoney"),max(col("VendorName")).alias("VendorName")).sort("TotalMoney",ascending=False).collect()

# COMMAND ----------

df.select("VendorName","VendorID").distinct().collect()

# COMMAND ----------

df2 = spark.read.load('abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/Clean/Location', format="parquet")

# COMMAND ----------

df.createOrReplaceTempView("Taxi")
df.cache()
df.count()


df2.createOrReplaceTempView("Location")
df2.cache()
df2.count()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.VendorID,
# MAGIC        l.Borough FromBorough,
# MAGIC        toL.Borough ToBorough,
# MAGIC        avg(t.TotalAmount),
# MAGIC        avg(t.PassengerCount),
# MAGIC        avg(t.TotalAmount)/avg(t.PassengerCount), 
# MAGIC        avg(t.TotalAmount/t.PassengerCount),
# MAGIC        sum(t.TotalAmount)/sum(t.PassengerCount),
# MAGIC        max(VendorName) as VendorName 
# MAGIC FROM Taxi t
# MAGIC   INNER JOIN Location l ON t.PickupLocationId = CAST(l.LocationID as integer)
# MAGIC   INNER JOIN Location toL ON t.DropOffLocationId = CAST(toL.LocationID as integer)
# MAGIC GROUP BY t.VendorID, l.Borough,toL.Borough
# MAGIC ORDER BY t.VendorID,l.Borough,toL.Borough

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Taxi WHERE TaxiTripKey IS NULL

# COMMAND ----------


