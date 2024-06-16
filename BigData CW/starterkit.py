import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, dayofmonth
from pyspark.sql.functions import to_date, count, col, month
from graphframes import *
from pyspark.sql.functions import from_unixtime, to_date, desc




if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    #Loading the data
    print("******************* Rideshare data *******************")
    rideshare_data=spark.read.format("csv").option("header", "true").load("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    print(rideshare_data.take(2))

    print("******************* Taxi Zone data *******************")
    taxi_zone_lookup=spark.read.format("csv").option("header", "true").load("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")
    print(taxi_zone_lookup.take(2))
    
    #Joining the data based on pickuplocation and LocationID
    pickup_join = rideshare_data.join(
    taxi_zone_lookup,
    rideshare_data.pickup_location == taxi_zone_lookup.LocationID,
    "left"
).select(
    rideshare_data["*"],
    col("Borough").alias("Pickup_Borough"),
    col("Zone").alias("Pickup_Zone"),
    col("service_zone").alias("Pickup_service_zone")
)

    #Joining the data based on dropofflocation and LocationID
    dropoff_join = pickup_join.join(
    taxi_zone_lookup,
    pickup_join.dropoff_location == taxi_zone_lookup.LocationID,
    "left"
).select(
    pickup_join["*"],
    col("Borough").alias("Dropoff_Borough"),
    col("Zone").alias("Dropoff_Zone"),
    col("service_zone").alias("Dropoff_service_zone")
)

    dropoff_join = dropoff_join.withColumn("date", to_date(from_unixtime(col("date"))))
    
    num_rows = dropoff_join.count()
    print(f"Number of rows in the DataFrame: {num_rows}")

    #Fetching the month from date column
    dropoff_join.printSchema()
    rideshare_with_month = dropoff_join.withColumn("month", month(col("date")))
    
    # ****** Task2 ******
    
    #Fetching monthly trip count for each business
    monthly_trips = rideshare_with_month.groupBy("business", "month").count().withColumnRenamed("count", "trip_count")
    monthly_trips.repartition(1).write.option("header", "true").mode("overwrite").csv("s3a://" + s3_bucket + "/monthly_trips.csv")

    #Converting rideshare_profit from string to float
    rideshare_with_month = rideshare_with_month.withColumn("rideshare_profit", col("rideshare_profit").cast("float"))

    #Fetching platform profit for each business
    monthly_profits = rideshare_with_month.groupBy('business', 'month').sum('rideshare_profit').withColumnRenamed('sum(rideshare_profit)', 'platform_profit')
    monthly_profits.repartition(1).write.option("header", "true").mode("overwrite").csv("s3a://" + s3_bucket + "/monthly_profits.csv")


    #Converting driver total pay to float    
    rideshare_with_month = rideshare_with_month.withColumn("driver_total_pay", col("driver_total_pay").cast("float"))
    drivers_earning = rideshare_with_month.groupBy('business', 'month').sum('driver_total_pay').withColumnRenamed('sum(driver_total_pay)', 'drivers_earnings')
    drivers_earning.repartition(1).write.option("header", "true").mode("overwrite").csv("s3a://" + s3_bucket + "/drivers_earnings.csv")

    # ****** Task3 ******

    rideshare_with_month.createOrReplaceTempView("pickup_data")
    top5_pickup_boroughs_sql = spark.sql("""
        SELECT Pickup_Borough, month AS Month, trip_count
        FROM (
            SELECT month,
                   Pickup_Borough,
                   COUNT(*) AS trip_count,
                   ROW_NUMBER() OVER (PARTITION BY month ORDER BY COUNT(*) DESC) AS rn
            FROM pickup_data
            GROUP BY month, Pickup_Borough
        ) ranked
        WHERE rn <= 5
        ORDER BY month, trip_count DESC
    """)
    top5_pickup_boroughs_sql.show(truncate = False)
    

    rideshare_with_month.createOrReplaceTempView("dropoff_data")
    top5_dropoff_boroughs_sql = spark.sql("""
        SELECT Dropoff_Borough, month AS Month, trip_count
        FROM (
            SELECT month,
                   Dropoff_Borough,
                   COUNT(*) AS trip_count,
                   ROW_NUMBER() OVER (PARTITION BY month ORDER BY COUNT(*) DESC) AS rn
            FROM dropoff_data
            GROUP BY month, Dropoff_Borough
        ) ranked
        WHERE rn <= 5
        ORDER BY month, trip_count DESC
    """)
    top5_dropoff_boroughs_sql.show(truncate = False)

    
    rideshare_with_month.createOrReplaceTempView("rideshares")
    top_30_routes = spark.sql("""
        SELECT CONCAT(Pickup_Borough, ' to ', Dropoff_Borough) AS Route,
               SUM(driver_total_pay) AS total_profit
        FROM rideshares
        GROUP BY Pickup_Borough, Dropoff_Borough
        ORDER BY total_profit DESC
        LIMIT 30
    """)
    top_30_routes.show(truncate=False)

    # ****** Task4 ******


    rideshare_with_month.createOrReplaceTempView("rideshares")

    avg_driver_total_pay = spark.sql("""
        SELECT time_of_day,
           AVG(driver_total_pay) AS average_drive_total_pay
    FROM rideshares
    GROUP BY time_of_day
    ORDER BY average_drive_total_pay DESC
    """)

    avg_driver_total_pay.show(5)

    rideshare_with_month = rideshare_with_month.withColumn("trip_length", col("trip_length").cast("float"))

    rideshare_with_month.createOrReplaceTempView("rideshares")

    average_trip_length = spark.sql("""
        SELECT time_of_day,
           AVG(trip_length) AS average_trip_length
    FROM rideshares
    GROUP BY time_of_day
    ORDER BY average_trip_length DESC
    """)

    average_trip_length.show(5)

    avg_earning_per_mile = avg_driver_total_pay.join(average_trip_length, "time_of_day") \
    .select("time_of_day", (col("average_drive_total_pay") / col("average_trip_length")).alias("average_earning_per_mile")) \
    .orderBy("average_earning_per_mile", ascending=False)


    avg_earning_per_mile.show(5)
    
    # ****** Task5 ******

    
    rideshare_with_month = rideshare_with_month.withColumn("request_to_pickup", col("request_to_pickup").cast("int"))
    january_data = rideshare_with_month.filter(col("month") == 1)
    january_data = january_data.withColumn("day", dayofmonth("date"))
    january_data.show(12)
 
    average_waiting_time = january_data.groupBy("day") \
                                       .avg("request_to_pickup") \
                                       .orderBy("day")
    average_waiting_time.repartition(1).write.option("header", "true").mode("overwrite").csv("s3a://" + s3_bucket + "/waiting_time.csv")

    # ****** Task 6 ******

    rideshare_with_month.createOrReplaceTempView("rideshares")
    
    trip_count = spark.sql("""
    SELECT Pickup_Borough, time_of_day, COUNT(*) AS trip_count
    FROM rideshares
    GROUP BY Pickup_Borough, time_of_day
    HAVING COUNT(*) > 0 AND COUNT(*) < 1000
    ORDER BY Pickup_Borough, time_of_day
""")

    trip_count.show(12)


    rideshare_with_month.createOrReplaceTempView("rideshares")
    
    trip_count_evening = spark.sql("""
    SELECT Pickup_Borough, time_of_day, COUNT(*) AS trip_count
    FROM rideshares
    WHERE time_of_day = 'evening'
    GROUP BY Pickup_Borough, time_of_day
""")

    trip_count_evening.show(12)

    
    rideshare_with_month.createOrReplaceTempView("rideshares")
    
    trip_Brooklyn_Staten_Island = spark.sql("""
        SELECT Pickup_Borough, Dropoff_Borough, Pickup_Zone
        FROM rideshares
        WHERE Pickup_Borough = 'Brooklyn' AND Dropoff_Borough = 'Staten Island'
        LIMIT 10;
        """)

    trip_Brooklyn_Staten_Island.show(12)
    
    
    spark.stop()
