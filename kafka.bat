@echo off
start zookeeper-server-start c:\kafka_2.12-3.9.0\config\zookeeper.properties
start kafka-server-start c:\kafka_2.12-3.9.0\config\server.properties
::start kafka-console-producer --topic twitter --bootstrap-server localhost:9092
::start kafka-console-consumer --topic twitter --from-beginning --bootstrap-server localhost:9092