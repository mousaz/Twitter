package edu.najah.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType, StructType, TimestampType}

object App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Twitter")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schema = new StructType()
      .add("key", BinaryType)
      .add("value", BinaryType)
      .add("topic", StringType)
      .add("partition", IntegerType)
      .add("offset", LongType)
      .add("timestamp", TimestampType)
      .add("timestampType", IntegerType)

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testtopic")
      .load()
      .selectExpr(
        "CAST(key AS BINARY)",
        "CAST(value AS STRING)",
        "topic",
        "partition",
        "offset",
        "timestamp",
        "timestampType"
      )

    val wordsCounts = df.groupBy($"value").count()

    val query = wordsCounts
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }
}
