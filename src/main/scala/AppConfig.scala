package edu.najah.bigdata

import scala.collection.mutable.Map
import scala.io.Source

object AppConfig {

  private val configs = Map[String, String]()

  def load(configFilePath: String): Unit = {
    val file = Source.fromFile(configFilePath)
    try {
      file
        .getLines()
        .map(_.trim)
        .filter(!_.startsWith("#"))
        .map {
          line => {
            val parts = line.split("=")
            if (parts.length != 2) {
              (null, null)
            } else {
              (parts(0).trim, parts(1).trim)
            }
          }
        }
        .filter(_._1 != null)
        .foreach {
          case (k, v) => configs(k) = v
        }
    } finally {
      file.close()
    }
  }

  def getConfig(name: String): String = {
    if (configs.contains(name)) configs(name) else null
  }

  def show(): Unit = {
    configs.foreach { case (k, v) => println(s"$k=$v") }
  }
}
