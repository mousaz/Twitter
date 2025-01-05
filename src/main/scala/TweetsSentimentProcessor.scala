package edu.najah.bigdata

import scala.concurrent.{Future, ExecutionContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import java.util.Base64

object TweetsSentimentProcessor {
  // Define an implicit ExecutionContext
  implicit val ec: ExecutionContext = ExecutionContext.global

  val MONGODB_CONNECTION_STRING = AppConfig.getConfig("MONGODB_CONNECTION_STRING")
  val MONGODB_DATABASE_NAME = AppConfig.getConfig("MONGODB_DATABASE_NAME")
  val MONGODB_COLLECTION_NAME = AppConfig.getConfig("MONGODB_COLLECTION_NAME")
  val KAFKA_TOPIC_NAME = AppConfig.getConfig("KAFKA_TOPIC_NAME")

  def start(): Future[Unit] = Future {
    val conf = new SparkConf()
      .setAppName("TweetsSentimentProcessor")
      .setMaster("local[*]")
      .set("spark.sql.shuffle.partitions", "2")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    println("TweetsSentimentProcessor: Started.")

    val schema = StructType(Array(
      StructField("_id", StringType),
      StructField("fullText", StringType),
      StructField("sentiment", IntegerType, nullable=true)
    ))

    val query = """{"sentiment": null}"""
    val ds = spark
      .readStream
      .format("mongodb")
      .option("spark.mongodb.connection.uri", MONGODB_CONNECTION_STRING)
      .option("spark.mongodb.database", MONGODB_DATABASE_NAME)
      .option("spark.mongodb.collection", MONGODB_COLLECTION_NAME)
      .option("change.stream.publish.full.document.only", "true")
      .option("pipeline", s"""[{"$$match": $query}]""")
      .schema(schema)
      .load()

    val sentimentAnalyzer = new SentimentAnalyzer
    val extractSentimentUdf = udf(
      (text: String) => sentimentAnalyzer.extractSentiment(text)
    )

    val tweetsWithSentiment = ds
      .withColumn("sentiment", extractSentimentUdf($"fullText"))

    val fullDataWriteOperation = tweetsWithSentiment
      .writeStream
      .outputMode("append")
      .format("mongodb")
      .option("checkpointLocation", s"data/checkpoints/$KAFKA_TOPIC_NAME/TweetsSentiment")
      .option("forceDeleteTempCheckpointLocation", "true")
      .option("spark.mongodb.connection.uri", MONGODB_CONNECTION_STRING)
      .option("spark.mongodb.database", MONGODB_DATABASE_NAME)
      .option("spark.mongodb.collection", MONGODB_COLLECTION_NAME)
      .option("replaceDocument", "false")
      .option("operationType", "update")
      .option("upsert", "true")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // val fullDataWriteOperation = ds
    //   .select("fullText")
    //   .writeStream
    //   .outputMode("update")
    //   .format("console")
    //   .trigger(Trigger.ProcessingTime("1 seconds"))
    //   .start()

    fullDataWriteOperation.awaitTermination()
  }
}
