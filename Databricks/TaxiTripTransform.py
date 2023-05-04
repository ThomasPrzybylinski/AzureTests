# Databricks notebook source
from pyspark.sql.functions import row_number,sha2,concat,concat_ws

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="datalake",key="datalakeeastkey")

spark.conf.set("fs.azure.account.auth.type.tprzybyldatalakeeast.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.tprzybyldatalakeeast.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.tprzybyldatalakeeast.dfs.core.windows.net", service_credential)

dbutils.fs.ls("abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/")

datalakeroot="abfss://datalake@tprzybyldatalakeeast.dfs.core.windows.net/"

# COMMAND ----------

df = spark.read.load(datalakeroot+'Landing/TaxiTrip', format='parquet')
df.createOrReplaceTempView("TaxiTrip_Stage")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  VendorID as VendorId,
# MAGIC         CASE WHEN VendorID == 1 THEN "Creative Mobile Technologies, LLC"
# MAGIC              WHEN VendorID == 2 THEN "VeriFone Inc."
# MAGIC              ELSE NULL END as VendorName,
# MAGIC         tpep_pickup_datetime as PickupTime,
# MAGIC         tpep_dropoff_datetime as DropoffTime,
# MAGIC         passenger_count as PassengerCount,
# MAGIC 		trip_distance as TripDistance,
# MAGIC 		RatecodeID as RatecodeID,
# MAGIC         CASE WHEN RatecodeID == 1 THEN "Standard Rate"
# MAGIC              WHEN RatecodeID == 2 THEN "JFK"
# MAGIC              WHEN RatecodeID == 3 THEN "Newark"
# MAGIC              WHEN RatecodeID == 4 THEN "Nassau or Westchester"
# MAGIC              WHEN RatecodeID == 5 THEN "Negotiated fare"
# MAGIC              WHEN RatecodeID == 6 THEN "Group ride"
# MAGIC              ELSE NULL END AS RateCodeName,
# MAGIC 		store_and_fwd_flag as StoreAndFwdFlag,
# MAGIC 		PULocationID as PickupLocationId,
# MAGIC 		DOLocationID as DropOffLocationId,
# MAGIC 		payment_type as PaymentType,
# MAGIC         CASE WHEN payment_type == 1 THEN "Credit card"
# MAGIC              WHEN payment_type == 2 THEN "Cash"
# MAGIC              WHEN payment_type == 3 THEN "No Charge"
# MAGIC              WHEN payment_type == 4 THEN "Dispute"
# MAGIC              WHEN payment_type == 5 THEN "Unknown"
# MAGIC              WHEN payment_type == 6 THEN "Voided trip"
# MAGIC              ELSE NULL END AS PaymentTypeName,
# MAGIC 		fare_amount as FareAmount,
# MAGIC 		extra as ExtraAmount,
# MAGIC 		mta_tax as MTATaxAmount,
# MAGIC 		tip_amount as TipAmount,
# MAGIC 		tolls_amount as TollsAmount,
# MAGIC 		improvement_surcharge as ImprovementSurchargeAmount,
# MAGIC 		total_amount as TotalAmount,
# MAGIC 		congestion_surcharge as CongestionSurcharge,
# MAGIC 		airport_fee as AirportFee,
# MAGIC         YEAR(tpep_pickup_datetime) as PickupYear
# MAGIC FROM TaxiTrip_Stage

# COMMAND ----------

TaxiTrip_Transform = _sqldf


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lit

newKeys = None
keysExist = True

try:
    dbutils.fs.ls(datalakeroot + "Keys/TaxiTrip")
except Exception as e:
    keysExist=False

HashedRows = TaxiTrip_Transform.withColumn('hash',sha2(concat_ws('\0|',*TaxiTrip_Transform.columns),256))
HashedRows.cache()

if not keysExist:
    #dbutils.fs.mkdirs(datalakeroot + "Keys/TaxiTrip")
    newKeys = HashedRows.select(row_number().over(Window.orderBy(lit(1))).alias('TaxiTripKey'),HashedRows.hash)
else:
    TaxiKeys = spark.read.load(datalakeroot+'Keys/TaxiTrip', format='orc')
    curMaxKey = TaxiKeys.agg({"TaxiTripKey" : 'max'}).collect()[0][0]
    newKeys = HashedRows.join(TaxiKeys,HashedRows.hash == TaxiKeys.hash, "left_anti").select((row_number().over(Window.orderBy(lit(1)))+curMaxKey).alias('TaxiTripKey'),HashedRows.hash)

newKeys.write.format('orc').option('orc.bloom.filter.columns','hash').option('orc.compress','snappy').mode("append").save(datalakeroot+'Keys/TaxiTrip')


# COMMAND ----------

TaxiKeys = spark.read.load(datalakeroot+'Keys/TaxiTrip', format='orc')
TaxiTripWithKeys = HashedRows.join(TaxiKeys,HashedRows.hash == TaxiKeys.hash,"inner").select(TaxiKeys.TaxiTripKey,*TaxiTrip_Transform.columns)

# COMMAND ----------

from delta.tables import *
currentTable = DeltaTable.forPath(spark, datalakeroot+'Clean/TaxiTrip')
currentTable.alias('TaxiTrip').merge(TaxiTripWithKeys.alias('newTaxiTrips'), 'TaxiTrip.TaxiTripKey = newTaxiTrips.TaxiTripKey').whenNotMatchedInsertAll().execute()

#TaxiTripWithKeys.write.partitionBy('PickupYear').format('delta').option("mergeSchema", "true").mode("overwrite").save(datalakeroot+'Clean/TaxiTrip')

# COMMAND ----------

currentTable.optimize().executeCompaction()
TaxiTripWithKeys.show(10)


# COMMAND ----------

newKeys.show(10)

# COMMAND ----------

TaxiKeys.show(10)

# COMMAND ----------

newKeys.unpersist()
HashedRows.unpersist()

# COMMAND ----------


