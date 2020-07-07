import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

if __name__ == "__main__":

    sc = SparkContext("local", "First App")
    spark = SparkSession.builder.appName('abc').getOrCreate()

    ###Data Injection

    #TLC Taxi Zone LocationID - Lookup DSV file for Airport
    tlc_lookup = spark.read.csv("/home/serendio/Documents/Prasaanth/Gerald/data/taxi+_zone_lookup.csv",header=True)

    #Airport names in New york as per the problem statment
    filter_zone = ["Newark Airport","JFK Airport","LaGuardia Airport"]
    filtered_airports = tlc_lookup.filter(F.col("Zone").isin(filter_zone)).select("LocationID")

    #Yello Cabs
    yc_all = spark.read.csv("/home/serendio/Documents/Prasaanth/Gerald/data/yellow_cab/*.csv",
             header=True,
             inferSchema=True).select("PULocationID","DOLocationID","tpep_pickup_datetime")

    yc_all = yc_all.withColumn('month', F.month('tpep_pickup_datetime'))

    yc_pickup = filtered_airports.join(yc_all,(filtered_airports.LocationID==yc_all.PULocationID)).groupBy('month',"PULocationID").agg(F.count(F.col('month')).alias("pickup_rides"))
    yc_drop = filtered_airports.join(yc_all,(filtered_airports.LocationID==yc_all.DOLocationID)).groupBy('month',"DOLocationID").agg(F.count(F.col('month')).alias("drop_rides"))

    #Memory optimization - drop yc_all - huge
    yc_all.unpersist()

    yc_airport = yc_pickup.join(yc_drop, ['month'], how='full')

    #Store the result in the partitioned as parquet file
    yc_airport.repartition(1).write.parquet("output/airport_rides/cab=yellow_cabs/")



    #FHV
    fhv_all = spark.read.csv("/home/serendio/Documents/Prasaanth/Gerald/data/fhv/*.csv",
             header=True,
             inferSchema=True).select("PULocationID","DOLocationID","pickup_datetime")

    fhv_all = fhv_all.withColumn('month', F.month('pickup_datetime'))

    fhv_pickup = filtered_airports.join(fhv_all,(filtered_airports.LocationID==fhv_all.PULocationID)).groupBy('month',"PULocationID").agg(F.count(F.col('month')).alias("pickup_rides"))
    fhv_drop = filtered_airports.join(fhv_all,(filtered_airports.LocationID==fhv_all.DOLocationID)).groupBy('month',"DOLocationID").agg(F.count(F.col('month')).alias("drop_rides"))

    #Memory optimization - drop yc_all - huge
    fhv_all.unpersist()

    fhv_airport = fhv_pickup.join(fhv_drop, ['month'], how='full')

    #Store the result in the partitioned as parquet file
    fhv_airport.repartition(1).write.parquet("output/airport_rides/cab=fhv/")
