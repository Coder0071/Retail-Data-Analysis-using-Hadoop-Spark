from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

print("modules_started")

# Initializing Spark Session #
spark= SparkSession \
       .builder.appName("retail_data_set") \
       .config("spark.streaming.stopGracefullyOnShutdown", "true") \
       .getOrCreate()
       
print("Spark session created, Time to connect to Kafka server")


# Read Input from kafka server 
kafka_df = spark.readStream.format("kafka") \
         .option("kafka.bootstrap.servers","18.211.252.152:9092") \
         .option("subscribe","real-time-project") \
         .option("inferSchema", "true") \
         .option("multiLine", "true") \
         .option("startingOffsets", "earliest") \
         .load()

## Defining custom schema to read the data
jsonSchema=StructType([StructField("invoice_no",LongType(),True),
    StructField("country",StringType(),True), 
    StructField("timestamp",TimestampType(),True),
    StructField("type",StringType(),True), 
    StructField("items",ArrayType(StructType([StructField("SKU",StringType(),True), 
                                            StructField("quantity",IntegerType(),True), 
                                            StructField("title",StringType(),True),  
                                            StructField("unit_price",DoubleType(),True)]),True),True)])
                               
print("Create Invoice DataFrames as per schema")

invoice_df= kafka_df.select(from_json(col("value").cast("string"),jsonSchema).alias("value")).select("value.*")


# UDF for calculating total_cost
def total_cost(items,type):
    total_cost=0
    for item in items:
        total_cost+=item["quantity"]*item["unit_price"]
    if type == "RETURN":
        return total_cost*(-1)
    else:
        return total_cost
    
    
# UDF for calculating total_items
def items_count(items):
    counts =0
    for item in items:
        counts+=item["quantity"]
    return counts
    
# UDF for calculating order type 
def is_order(type):
    if type== "ORDER":
        return 1
    else:
        return 0
        
# UDF for calculating return type
def is_return(type):
    if type=="RETURN":
        return 1
    else:
        return 0
        
# Converting to UDF’s with the utility functions
totalcost= udf(total_cost,DoubleType())
totalitems = udf(items_count,IntegerType())
isorder = udf(is_order,IntegerType())
isreturn = udf(is_return,IntegerType())


print("Writing the addtional columns to kafka data stream")

invoice_df = invoice_df \
        .withColumn("total_cost", totalcost(invoice_df.items, invoice_df.type)) \
        .withColumn("total_items", totalitems(invoice_df.items)) \
        .withColumn("is_order", isorder(invoice_df.type)) \
        .withColumn("is_return", isreturn(invoice_df.type)) 

print("Creating Invoice Retail-dataframe")

# Writing the Intermediary data into Console

retailstream = invoice_df \
       .select("invoice_no", "country", "timestamp","total_cost","total_items","is_order","is_return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()

## Calculating time based KPI
timebasedKPIS = invoice_df \
            .withWatermark("timestamp","1 minute") \
            .groupby(window("timestamp", "1 minute", "1 minute")) \
            .agg(count("invoice_no").alias("OPM"),
                 sum("total_cost").alias("total_sales_volume"),
                 avg("total_cost").alias("average_transaction_size"),
                 avg("is_return").alias("rate_of_return")) \
            .select("window", "OPM", "total_sales_volume", "average_transaction_size", "rate_of_return")

# write stream data in json format for time based KPIs
timebasedKPIS_output = timebasedKPIS \
    .writeStream \
    .outputMode("Append") \
    .format("json") \
    .option("format","append") \
    .option("path", "time_KPI") \
    .option("checkpointLocation", "time-KPI") \
    .option("truncate", "False") \
    .trigger(processingTime="1 minute") \
    .start() 

# Calculating time and country-based KPIs 

timecountryKPIS = invoice_df \
    .withWatermark("timestamp", "1 minute") \
    .groupby(window("timestamp", "1 minute", "1 minute"), "country") \
    .agg(count("invoice_no").alias("OPM"),
         sum("total_cost").alias("total_sales_volume"), 
         avg("is_return").alias("rate_of_return")) \
    .select("window", "country", "OPM", "total_sales_volume", "rate_of_return") 

## Write stream data in json format for time and country based KPIS
timecountryKPIS_output = timecountryKPIS \
    .writeStream \
    .outputMode("Append") \
    .format("json") \
    .option("format","append") \
    .option("truncate", "false") \
    .option("path", "timecountry_KPI") \
    .option("checkpointLocation","time-country-KPI") \
    .trigger(processingTime="1 minute") \
    .start()    
    
## Spark to await termination
retailstream.awaitTermination()   
timebasedKPIS_output.awaitTermination()
timecountryKPIS_output.awaitTermination()

#### The below command is used to run this file in EMR cluster, the command is commented as this is for reference executed 
#   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 spark-streaming.py 18.211.252.152 9092 real-time-project
