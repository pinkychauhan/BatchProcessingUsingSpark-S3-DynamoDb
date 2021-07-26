#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, FloatType
import pyspark.sql.functions as func


spark = SparkSession.builder.appName("ccc_task1").enableHiveSupport().getOrCreate()

schema = StructType([ \
    StructField("Year",IntegerType(),True), \
    StructField("Month",IntegerType(),True), \
    StructField("DayofMonth", IntegerType(), True), \
    StructField("DayOfWeek", IntegerType(), True), \
    StructField("FlightDate", DateType(), True), \
    StructField("UniqueCarrier",StringType(),True), \
    StructField("FlightNum",IntegerType(),True), \
    StructField("Origin",StringType(),True), \
    StructField("Dest", StringType(), True), \
    StructField("CRSDepTime", IntegerType(), True), \
    StructField("DepTime", FloatType(), True), \
    StructField("DepDelay",FloatType(),True), \
    StructField("DepDelayMinutes",FloatType(),True), \
    StructField("CRSArrTime",IntegerType(),True), \
    StructField("ArrTime", FloatType(), True), \
    StructField("ArrDelay", FloatType(), True), \
    StructField("ArrDelayMinutes", FloatType(), True), \
    StructField("Cancelled", FloatType(), True) \
  ])


df =  spark.read.format("csv") \
      .option("header", "true") \
      .schema(schema) \
      .load("hdfs:///aviation")

df.printSchema()

df.createOrReplaceTempView("aviation");

# Group 1

#1. Rank the top 10 most popular airports by numbers of flights to/from the airport

print("Start execution: Group1 Question1")

spark.sql("CREATE TABLE IF NOT EXISTS group1_q1 USING hive as select src.Airport, (src.count + des.count) as FlightsCount \
from \
(select Origin as Airport, count(*) as count from aviation group by Origin) src \
join \
(select Dest as Airport, count(*) as count from aviation group by Dest) des \
ON src.Airport = des.Airport \
order by FlightsCount desc \
limit 10")

print("Finish execution: Group1 Question1")

#2. Rank the top 10 airlines by on-time arrival performance.

print("Start execution: Group1 Question2")

spark.sql("CREATE TABLE IF NOT EXISTS group1_q2 USING hive as select UniqueCarrier as Airline, \
round(avg(ArrDelay),2) as AvgArrDelay \
from aviation \
where Cancelled = 0 \
group by UniqueCarrier order by AvgArrDelay asc limit 10")

print("Finish execution: Group1 Question2")

# Group 2:

#1. For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.

print("Start execution: Group2 Question1")

spark.sql("CREATE TABLE IF NOT EXISTS group2_q1 USING hive as select Origin as SrcAirport, UniqueCarrier as Airline, AvgDepDelay \
from (select Origin, UniqueCarrier, AvgDepDelay, row_number() over (partition by Origin order by AvgDepDelay) as Seqnum \
      from (select Origin, UniqueCarrier, round(avg(DepDelay),2) as AvgDepDelay \
            from aviation where Cancelled = 0 group by Origin, UniqueCarrier \
      ) \
) \
where Seqnum <= 10 \
order by SrcAirport, AvgDepDelay ")

print("Finish execution: Group2 Question1")


#2. For each source airport X, rank the top-10 destination airports in decreasing order of on-time departure performance from X.

print("Start execution: Group2 Question2")

spark.sql("CREATE TABLE IF NOT EXISTS group2_q2 USING hive as select Origin as SrcAirport, Dest as DestAirport, AvgDepDelay \
from (select Origin, Dest, AvgDepDelay, row_number() over (partition by Origin order by AvgDepDelay) as Seqnum \
      from (select Origin, Dest, round(avg(DepDelay),2) as AvgDepDelay \
            from aviation where Cancelled = 0 group by Origin, Dest \
      ) \
) \
where Seqnum <= 10 \
order by SrcAirport, AvgDepDelay ")

print("Finish execution: Group2 Question2")


#3. For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.

print("Start execution: Group2 Question3")

spark.sql("CREATE TABLE IF NOT EXISTS group2_q3 USING hive as select Src_Dest_Pair, UniqueCarrier as Airline, AvgArrDelay \
from (select Src_Dest_Pair, UniqueCarrier, AvgArrDelay, row_number() over (partition by Src_Dest_Pair order by AvgArrDelay) as Seqnum \
      from (select concat(Origin, '-', Dest) as Src_Dest_Pair, UniqueCarrier, round(avg(ArrDelay),2) as AvgArrDelay \
            from aviation where Cancelled = 0 and ArrDelay is not null group by Src_Dest_Pair, UniqueCarrier \
      ) \
) \
where Seqnum <= 10 \
order by Src_Dest_Pair, AvgArrDelay ")

print("Finish execution: Group2 Question3")


# Group 3:

#1. Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?
# airports_popularity_dist = spark.sql("select src.Airport, (src.count + des.count) as FlightsCount \
# from (select Origin as Airport, count(*) as count from aviation group by Origin) src \
# left outer join (select Dest as Airport, count(*) as count from aviation group by Dest) des \
# ON src.Airport = des.Airport \
# order by FlightsCount desc ")

#airports_popularity_dist.toPandas().plot(kind='scatter',x='Airport',y='FlightsCount',color='blue', loglog=True)
#plt.show()

#2. Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y on the way.
#More concretely, Tom has the following requirements
# a) The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y).
#    For example, if X-Y departs on January 5, 2008, Y-Z must depart on January 7, 2008.
# b) Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
# c) Tom wants to arrive at each destination with as little delay as possible. You can assume you know the actual delay of each flight.

# Find for each X-Y-Z and day/month (dd/mm) combination in the year 2008, the two flights (X-Y and Y-Z) that satisfy constraints (a) and (b) and have the best individual performance with respect to constraint (c), if such flights exist.



print("Start execution: Group3 Question2")

spark.sql("CREATE TABLE IF NOT EXISTS group3_q2 USING hive as select concat(a.Origin, '-', a.Dest, '-', b.Dest) as XYZ, \
concat(lpad(a.DayOfMonth, 2, '0'), '/', lpad(a.Month, 2, '0'),'/', a.Year) as StartDate, \
(a.ArrDelay + b.ArrDelay) as TotalArrDelay, \
a.Origin as FirstLeg_Origin, \
a.Dest as FirstLeg_Destn, \
concat(a.UniqueCarrier, ' ', a.FlightNum) as FirstLeg_AirlineFlightNum, \
concat(a.FlightDate, ' ', a.CRSDepTime) as FirstLeg_SchedDepart, \
a.ArrDelay as FirstLeg_ArrDelay, \
b.Origin as SecondLeg_Origin, \
b.Dest as SecondLeg_Destn, \
concat(b.UniqueCarrier, ' ', b.FlightNum) as SecondLeg_AirlineFlightNum, \
concat(b.FlightDate, ' ', b.CRSDepTime) as SecondLeg_SchedDepart, \
b.ArrDelay as SecondLeg_ArrDelay \
from \
(select DayOfMonth, Month, Year, Origin, Dest, UniqueCarrier, FlightNum, FlightDate, CRSDepTime, ArrDelay, \
 row_number() over (partition by Origin, Dest, DayOfMonth, Month, Year order by ArrDelay) as Seqnum \
 from aviation where Year = 2008 and CRSDepTime < 1200 and ArrDelay is not null) a \
join \
(select DayOfMonth, Month, Year, Origin, Dest, UniqueCarrier, FlightNum, FlightDate, CRSDepTime, ArrDelay, \
 row_number() over (partition by Origin, Dest, DayOfMonth, Month, Year order by ArrDelay) as Seqnum \
 from aviation where Year = 2008 and CRSDepTime > 1200 and ArrDelay is not null) b \
on a.Dest = b.Origin and date_add(a.FlightDate, 2) = b.FlightDate and a.Seqnum = 1 and b.Seqnum = 1 ")

print("Finish execution: Group3 Question2")

#result_3_2 = spark.sql("SELECT * from group3_q2")
#result_3_2.write.format('csv').option('header','true').save('s3a://bucket/group3_q2',mode='overwrite')

#print("Finish loading results to S3: Group3 Question2")
