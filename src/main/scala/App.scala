package edu.najah.bigdata

import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._

object App {
  // Define an implicit ExecutionContext
  implicit val ec: ExecutionContext = ExecutionContext.global

  def main(args: Array[String]): Unit = {
    // Set the system property for the logging configuration file
    // change the logging level within the file to get more logs
    // the level is set to "error" to suppress extensive logging from spark
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")

    // Load application configuration
    AppConfig.load("src/main/resources/AppConfig.properties")
    println("Using AppConfig:")
    AppConfig.show()

    Await.result(Future.sequence(List(
      TweetsConsumer.start(),
      TweetsSentimentProcessor.start()
    )), Duration.Inf)
  }
}
