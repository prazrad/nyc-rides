import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

if __name__ == "__main__":

    sc = SparkContext("local", "First App")
    spark = SparkSession.builder.appName('abc').getOrCreate()

    ##Data Injection
    #Yello Cabs
    yc_all = spark.read.csv("/<your-path>/Prasaanth/Gerald/data/yellow_cab/*.csv",
             header=True,
             inferSchema=True).select("PULocationID","DOLocationID","tpep_pickup_datetime","tpep_dropoff_datetime","Trip_distance")

    #Calculate duration between two total_rides
    timeFmt = "yyyy-MM-dd HH:mm:ss"
    timeDiff = (F.unix_timestamp('tpep_dropoff_datetime', format=timeFmt) - F.unix_timestamp('tpep_pickup_datetime', format=timeFmt))

    yc_all = yc_all.withColumn("Duration", timeDiff).withColumn('month', F.month('tpep_pickup_datetime'))

    yc_summary = yc_all.groupBy('month').agg(F.count(F.col('month')).alias("total_rides"), F.sum(F.col('Duration')).alias('total_duration'), F.sum(F.col('Trip_distance')).alias('total_distance'))


    #Memory optimization - drop yc_all - huge
    yc_all.unpersist()

    yc_summary.repartition(1).write.parquet("output/all_rides/cab=yellow_cabs/")


    #For Hire Vehicle
    fhv_all = spark.read.csv("/<your-path>/Prasaanth/Gerald/data/fhv/*.csv",
             header=True,
             inferSchema=True).select("PULocationID","DOLocationID","pickup_datetime","dropoff_datetime")

    #Calculate duration between two total_rides
    timeFmt = "yyyy-MM-dd HH:mm:ss"
    timeDiff = (F.unix_timestamp('dropoff_datetime', format=timeFmt) - F.unix_timestamp('pickup_datetime', format=timeFmt))

    fhv_all = fhv_all.withColumn("Duration", timeDiff).withColumn('month', F.month('pickup_datetime'))

    fhv_summary = fhv_all.groupBy('month').agg(F.count(F.col('month')).alias("total_rides"), F.sum(F.col('Duration')).alias('total_duration'))

    #Memory optimization - drop yc_all - huge
    fhv_all.unpersist()

    fhv_summary.repartition(1).write.parquet("output/all_rides/cab=fhv/")
