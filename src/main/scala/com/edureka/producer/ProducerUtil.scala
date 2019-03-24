package com.edureka.producer

import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
//cd /home/edureka/SAIWS/BATCHES
//spark-submit --verbose --master local --deploy-mode client --conf spark.driver.extraClassPath=/home/edureka/SAIWS/BATCHES/mysql-connector-java-5.1.45-bin.jar:/usr/lib/kafka_2.12-0.11.0.0/libs/kafka-clients-0.11.0.0.jar --class com.laboros.spark.sql.SparkSqlProducer sparksqldemo_2.11-1.0.jar kafka_topic
object ProducerUtil {
  
  val mandatoryOptions: Map[String, Any] = Map(
      "bootstrap.servers" -> "ip-20-0-31-210.ec2.internal:9092",
      "acks" -> "all",
      "batch.size" -> 16384,
      "buffer.memory" -> 33554432,
      "group.id" -> "BATCH09022019-Kafka-SparkListener",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
  
  
  def extractOptions(properties: Map[String, Any]): Properties = {
    val props = new Properties()
    properties.foreach { case (key, value) => props.put(key, value.toString) }
    props
  }
  
  def getProducer():KafkaProducer[String, String] = {
     getProducer(mandatoryOptions)
  }
  
  def getProducer(properties: Map[String, Any]): KafkaProducer[String, String] = {
    val props = extractOptions(properties);
    new KafkaProducer[String, String](props);
  }

  def close(kafkaProducer: KafkaProducer[String, String]): Unit = kafkaProducer.close()

  def send(kafkaProducer: KafkaProducer[String, String], topic: String, message: String): Unit = {
    val record = new ProducerRecord(topic, "", message)
    kafkaProducer.send(record)
  }
  
  def send(kafkaProducer: KafkaProducer[String, String], topic: String, key: String,value:String): Unit = {
    val record = new ProducerRecord(topic, key, value)
    kafkaProducer.send(record)
  }
  
  
  def produceDataFromJDF(spark:SparkSession, topic:String,props:Map[String,Any])
  {
    val jdbcUsername = "root"
    val jdbcPassword = "Edurekasql_123"
    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase = "mydb"
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"

    val conProps = new Properties()
    conProps.put("user", "root")
    conProps.put("password", "Edurekasql_123")
    conProps.put("driver", "com.mysql.jdbc.Driver")

    val edf = spark.read.jdbc(jdbcUrl, "elections", conProps);
    
    import spark.implicits._;
    
    val selectDF = edf.select("fips", "Households").limit(10);
    
    selectDF.foreach(r=> {
//      send(kp,topic,r.getAs[String]("fips")+","+r.getAs[String]("Households"))
      val producer: KafkaProducer[String, String] = getProducer(props);
      producer.send(new ProducerRecord(topic,r.getString(0)+","+r.getInt(1).toString()))
       close(producer)
    });

  }
}