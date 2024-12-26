ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "Twitter",
    idePackagePrefix := Some("edu.najah.bigdata"),
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17",
      "org.apache.spark" %% "spark-core" % "3.5.4",
      "org.apache.spark" %% "spark-sql" % "3.5.4",
      "org.apache.spark" %% "spark-mllib" % "3.5.4",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.4",
      "org.apache.kafka" % "kafka-clients" % "3.7.0",
      "org.mongodb.scala" %% "mongo-scala-driver" % "5.2.1"
    )
  )
