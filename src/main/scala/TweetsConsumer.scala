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

object TweetsConsumer {

  val TOPIC_NAME = "twitter-test"
  val MONGODB_CONNECTION_STRING = "mongodb://localhost:27017/"
  val MONGODB_DATABASE_NAME = "TwitterTest"

  case class Schema(
    key: String,
    value: String,
    topic: String,
    partition: String,
    offset: String,
    timestamp: String,
    timestampType: String)
  
  case class Tweet(
    fullText: String,
    plainText: String,
    createdAtUTC: Timestamp,
    hashTags: Seq[String],
    latitude: Double,
    longitude: Double,
    country: String,
    countryCode: String,
    placeType: String,
    placeName: String,
    retweetCount: Integer
  )

  def main(args: Array[String]): Unit = {
    // Set the system property for the logging configuration file
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")

    val conf = new SparkConf()
      .setAppName("Twitter")
      .setMaster("local[*]")
      .set("spark.sql.shuffle.partitions", "2")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val ds = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", TOPIC_NAME)
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

    // val ds = spark
    //   .read
    //   .text("data/boulder_flood_geolocated_tweets.json")
    //   .as[String]

    val parsedTweets = ds.map(t => extractTweet(t.value))
    // val parsedTweets = ds.map(extractTweet(_))
    // pipeline.transform(parsedTweets.select("fullText")).show(false)

    // val fullData = parsedTweets
    //   .map(t => t.copy(hashTags = t.hashTags.map(_.toLowerCase())))
    //   .withColumn("hashTag", explode($"hashTags"))
    //   .withColumn("city", when($"placeType" === "city", $"placeName").otherwise(""))
    //   .withColumn("admin", when($"placeType" === "admin", $"placeName").otherwise(""))
    //   .withColumn("neighborhood", when($"placeType" === "neighborhood", $"placeName").otherwise(""))
    //   .drop($"hashTags", $"placeType", $"placeName")
   
    // val hashTagsHourly = fullData
    //   .withColumn("createdAtHour", date_trunc("hour", $"createdAtUTC"))
    //   .groupBy($"hashTag", $"createdAtHour")
    //   .count()

    // val hashTagsDaily = fullData
    //   .withColumn("createdAtHour", date_trunc("day", $"createdAtUTC"))
    //   .groupBy($"hashTag", $"createdAtHour")
    //   .count()

    val sentimentAnalyzer = new SentimentAnalyzer
    val extractSentimentUdf = udf(
      (text: String) => sentimentAnalyzer.extractSentiment(text)
    )

    val tweetsWithSentiment = parsedTweets
      .withColumn("sentiment", extractSentimentUdf($"fullText"))

    val fullDataWriteOperation = tweetsWithSentiment
      .writeStream
      .outputMode("append")
      .format("mongodb")
      .option("checkpointLocation", s"data/checkpoints/$TOPIC_NAME/Tweets")
      .option("forceDeleteTempCheckpointLocation", "true")
      .option("spark.mongodb.connection.uri", MONGODB_CONNECTION_STRING)
      .option("spark.mongodb.database", MONGODB_DATABASE_NAME)
      .option("spark.mongodb.collection", "Tweets")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()
    
    // val hashTagsHourlyWriteOperation = hashTagsHourly
    //   .writeStream
    //   .outputMode("append")
    //   .format("mongodb")
    //   .option("checkpointLocation", s"data/checkpoints/$TOPIC_NAME/HashTagsHourly")
    //   .option("forceDeleteTempCheckpointLocation", "true")
    //   .option("spark.mongodb.connection.uri", MONGODB_CONNECTION_STRING)
    //   .option("spark.mongodb.database", MONGODB_DATABASE_NAME)
    //   .option("spark.mongodb.collection", "HashTagsHourly")
    //   .option("replaceDocument", "false")
    //   .option("operationType", "update")
    //   .option("upsert", "true")
    //   .trigger(Trigger.ProcessingTime("2 seconds"))
    //   .start()

    // val hashTagsDailyWriteOperation = hashTagsDaily
    //   .writeStream
    //   .outputMode("append")
    //   .format("mongodb")
    //   .option("checkpointLocation", s"data/checkpoints/$TOPIC_NAME/HashTagsDaily")
    //   .option("forceDeleteTempCheckpointLocation", "true")
    //   .option("spark.mongodb.connection.uri", MONGODB_CONNECTION_STRING)
    //   .option("spark.mongodb.database", MONGODB_DATABASE_NAME)
    //   .option("spark.mongodb.collection", "HashTagsDaily")
    //   .option("replaceDocument", "false")
    //   .option("operationType", "update")
    //   .option("upsert", "true")
    //   .trigger(Trigger.ProcessingTime("2 seconds"))
    //   .start()

    fullDataWriteOperation.awaitTermination()
    // hashTagsHourlyWriteOperation.awaitTermination()
    // hashTagsDailyWriteOperation.awaitTermination()
    
    // val query = wordsCounts
    //   .writeStream
    //   .outputMode("update")
    //   .format("console")
    //   .trigger(Trigger.ProcessingTime("1 seconds"))
    //   .start()

    // query.awaitTermination()
  }

  def extractTweet(value: String): Tweet = {
    import ujson._
    val parsedJson = ujson.read(value)
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
