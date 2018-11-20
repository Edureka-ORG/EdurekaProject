package com.edureka.consumer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import com.edureka.producer.ProducerUtil
import org.apache.kafka.common.serialization.StringDeserializer

object SparkStreamingConsumer {
  def main(args: Array[String]): Unit =
    {

      //Step-1 : Create SparkStreaming Context

      val sparkConf = new SparkConf().setAppName("Spark-Streaming-Helloworld").setMaster("local[*]").set("spark.ui.enabled", "true").set("spark.submit.deployMode", "client");

      val sc = new SparkContext(sparkConf);

      val mandatoryOptions: Map[String, Object] = Map(
        "bootstrap.servers" -> "ip-20-0-31-210.ec2.internal:9092",
        "acks" -> "all",
        "group.id" -> "BATCH291018-Kafka-SparkListener",
        "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "earliest",
        "enable.auto.commit" -> (false: java.lang.Boolean))

      sc.setLogLevel("INFO");

      val ssc = new StreamingContext(sc, Seconds(5));

      val topics = Array("BATCH29102018-TOPIC");

      val dStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, mandatoryOptions));

      //Create InputDStreams

      dStream.foreachRDD { rdd =>
        rdd.foreach(CR => println(CR.key() + "," + CR.value()));
      }

      ssc.start();
      ssc.awaitTermination()

    }

}