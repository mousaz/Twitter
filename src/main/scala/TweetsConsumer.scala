package edu.najah.bigdata

import java.time.ZonedDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations
import java.util.Properties
import scala.jdk.CollectionConverters._
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import scala.concurrent.Future
import scala.util.Success
import scala.concurrent.ExecutionContext

object TweetsConsumer {

  val MONGODB_CONNECTION_STRING = AppConfig.getConfig("MONGODB_CONNECTION_STRING")
  val MONGODB_DATABASE_NAME = AppConfig.getConfig("MONGODB_DATABASE_NAME")
  val MONGODB_COLLECTION_NAME = AppConfig.getConfig("MONGODB_COLLECTION_NAME")
  val KAFKA_TOPIC_NAME = AppConfig.getConfig("KAFKA_TOPIC_NAME")
  val BLOOM_FILTER_SIZE = AppConfig.getConfig("BLOOM_FILTER_SIZE").toInt
  val BLOOM_FILTER_NUM_HASHES = AppConfig.getConfig("BLOOM_FILTER_NUM_HASHES").toInt

  case class Schema(
    key: String,
    value: String,
    topic: String,
    partition: String,
    offset: String,
    timestamp: String,
    timestampType: String)
  
  // Define an implicit ExecutionContext
  implicit val ec: ExecutionContext = ExecutionContext.global

  def start(): Future[Unit] = Future {
    val bloomFilter = new BloomFilter(BLOOM_FILTER_SIZE, BLOOM_FILTER_NUM_HASHES)

    val conf = new SparkConf()
      .setAppName("TweetsConsumer")
      .setMaster("local[*]")
      .set("spark.sql.shuffle.partitions", "2")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    println("TweetsConsumer: Started.")
    val ds = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", KAFKA_TOPIC_NAME)
      .load()
      .selectExpr(
        "CAST(key AS STRING)",
        "CAST(value AS STRING)",
        "topic",
        "partition",
        "offset",
        "timestamp",
        "timestampType"
      )
      .as[Schema]

    val parsedTweets = ds
      .map(t => extractTweet(t.value))
      .dropDuplicates("_id")
      .filter(t => !bloomFilter.hasSeenBefore(t._id))

    // Initialize sentiment column with null value for analysis later
    val tweetsWithSentiment = parsedTweets
      .withColumn("sentiment", lit(null))

    val fullDataWriteOperation = tweetsWithSentiment
      .writeStream
      .foreachBatch((batch: Dataset[Row], batchId: Long) => {
        batch.foreach(r => bloomFilter.setAsSeen(r.getAs[String]("id")))
      })
      .outputMode("append")
      .format("mongodb")
      .option("checkpointLocation", s"data/checkpoints/$KAFKA_TOPIC_NAME/Tweets")
      .option("forceDeleteTempCheckpointLocation", "true")
      .option("spark.mongodb.connection.uri", MONGODB_CONNECTION_STRING)
      .option("spark.mongodb.database", MONGODB_DATABASE_NAME)
      .option("spark.mongodb.collection", MONGODB_COLLECTION_NAME)
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    fullDataWriteOperation.awaitTermination()
  }

  def extractTweet(value: String): Tweet = {
    import ujson._
    val parsedJson = ujson.read(value)
    val id = parsedJson("id_str").str
    val fullTweet = parsedJson("text").str
    val retweetCount = parsedJson("retweet_count").num.toInt

    val place = parsedJson("place")

    def extractStringFieldFromJsonObj(place: Value, fieldName: String): String = {
      if (!place.isNull && !place.obj(fieldName).isNull) place.obj(fieldName).str else ""
    }

    val country = extractStringFieldFromJsonObj(place, "country")
    val countryCode = extractStringFieldFromJsonObj(place, "country_code")
    val placeType = extractStringFieldFromJsonObj(place, "place_type")
    val placeName = extractStringFieldFromJsonObj(place, "name")

    val geo = parsedJson("geo")
    var point = Array(0.0, 0)
    if (!geo.isNull) {
      point = geo.obj("coordinates").arr.map(_.num).toArray
    }

    val latitude = point(0)
    val longitude = point(1)

    val createdAt = Timestamp.from(
      ZonedDateTime.parse(
        parsedJson("created_at").str, 
        DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy")
      )
      .withZoneSameInstant(ZoneId.of("UTC"))
      .toInstant
    )

    val entities = parsedJson("entities").obj

    val hashTags = entities("hashtags")
      .arr
      .map(
        _.obj("text").str
      )
      .toSeq
    
    val userMentions = entities("user_mentions")
      .arr
      .map(
        _.obj("screen_name").str
      )
      .toSeq

    val symbols = entities("symbols")
      .arr
      .map(
        _.obj("text").str
      )
      .toSeq

    val urls = entities("urls")
      .arr
      .map(
        _.obj("url").str
      )
      .toSeq

    var textOnly = fullTweet
    hashTags.foreach(h => textOnly = textOnly.replaceAll(s"#$h", ""))
    symbols.foreach(s => textOnly = textOnly.replaceAll(s"$$$s", ""))
    urls.foreach(u => textOnly = textOnly.replaceAll(s"$u", ""))
    userMentions.foreach(u => textOnly = textOnly.replaceAll(s"@$u", ""))

    Tweet(
      id,
      fullTweet,
      textOnly,
      createdAt,
      hashTags,
      latitude,
      longitude,
      country,
      countryCode,
      placeType,
      placeName,
      retweetCount)
  }
}
