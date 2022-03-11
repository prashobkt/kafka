from pyspark.sql import SparkSession
from pyspark.sql.functions import get_json_object, from_csv, col

import os

# spark_version = '3.2.1'./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:{}'.format(spark_version)


if __name__ == '__main__':

    spark = SparkSession.builder.appName("myapp").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    payloads =  spark .readStream .format("kafka") .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
            .option("subscribe", "REALTIME") \
            .option("startingOffsets", "earliest")\
             .load()
    #payloads.select(get_json_object("$value").cast("string")).show()
    y= payloads.selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)")
    #selectExpr( "CAST(value AS STRING)")
    # schema_string = "source_key STRING,stream STRING,value DOUBLE,mf INT,date STRING"
    # x=y.select(from_csv(col("value"),schema_string)).alias("nx_val")
    # x.createOrReplaceTempView("find")
    # xy=spark.sql("select * from dfind")
    mn=y.writeStream.trigger(processingTime='5 seconds').format("console").start()
        # trigger(processingTime='5 seconds') \
        # .outputMode("append") \
        # .option("truncate", "false") \
        # .format("memory") \
        # .queryName("testedTable") \
        # .start()
    mn.awaitTermination()




