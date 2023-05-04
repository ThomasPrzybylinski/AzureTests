# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="datalake",key="datalakeeastkey")

spark.conf.set("fs.azure.account.auth.type.tprzybyldatalakeeast.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.tprzybyldatalakeeast.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.tprzybyldatalakeeast.dfs.core.windows.net", service_credential)

dbutils.fs.ls("abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/")

datalakeroot="abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/"

# COMMAND ----------

TaxiTrip = spark.range(1,40000000)

# COMMAND ----------

TaxiTripHashedRows = TaxiTrip.withColumn('hash',F.sha2(F.concat_ws('\0|',*TaxiTrip.columns),256))
HashedRows = TaxiTripHashedRows.select('hash')
HashedRows.cache()
HashedRows.count()

# COMMAND ----------

curMaxKey=12345678

# COMMAND ----------

from pyspark.sql.window import Window
newKeys = HashedRows.select((F.row_number().over(Window.orderBy(F.lit(1))) + F.lit(curMaxKey+1)).alias('TaxiTripKey'),HashedRows.hash)

# COMMAND ----------

from pyspark.rdd import StorageLevel
hashRDD = HashedRows.rdd
hashRDD.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------


partitionSizes = hashRDD.mapPartitions(lambda iter: [sum(1 for _ in iter)],True).collect()
partitionSizes
#newKeys2 = 

# COMMAND ----------

from pyspark import TaskContext
def populateKeys(index, iterator, partitionSizes, curMaxKey=0):
    curFirstIndex = 1+curMaxKey+sum(partitionSizes[:index])
    return zip(iterator, [curFirstIndex + x for x in range(partitionSizes[index])])
    

# COMMAND ----------

from functools import partial
populateKeysLocal = partial(populateKeys,partitionSizes=partitionSizes,curMaxKey=curMaxKey)
newKeys2 = hashRDD.mapPartitionsWithIndex(populateKeysLocal)
newKeys2 = newKeys2.toDF().select('_1.*',F.col('_2').alias('TaxiTripKey'))

# COMMAND ----------

newKeys3 = hashRDD.zipWithUniqueId()
newKeys3 = newKeys3.toDF().select('_1.*',(F.col('_2') + F.lit(curMaxKey+1)).alias('TaxiTripKey'))

# COMMAND ----------

newKeys4 = HashedRows.select((F.monotonically_increasing_id() + F.lit(curMaxKey+1)).alias('TaxiTripKey'),HashedRows.hash)


# COMMAND ----------

print(newKeys.select(F.min('TaxiTripKey'),F.max('TaxiTripKey')).collect())


# COMMAND ----------

print(newKeys2.select(F.min('TaxiTripKey'),F.max('TaxiTripKey')).collect())


# COMMAND ----------

print(newKeys3.select(F.min('TaxiTripKey'),F.max('TaxiTripKey')).collect())


# COMMAND ----------

print(newKeys4.select(F.min('TaxiTripKey'),F.max('TaxiTripKey')).collect())

# COMMAND ----------


