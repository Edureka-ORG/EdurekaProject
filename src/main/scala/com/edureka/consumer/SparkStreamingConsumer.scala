package com.edureka.consumer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

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

      val topicMap = "BATCH29102018-TOPIC".split(",").map((_, 1)).toMap

      val lines = KafkaUtils.createStream(
        ssc,
        "ip-20-0-21-161.ec2.internal:2181 ",
        "spark-streamingconsumer",
        topicMap).map(_._2)

      //      val dStream = KafkaUtils.createDirectStream[String, String](
      //        ssc,
      //        LocationStrategies.PreferConsistent,
      //        ConsumerStrategies.Subscribe[String, String](topics, mandatoryOptions));

      //Create InputDStreams

      lines.foreachRDD { rdd =>
        if (!rdd.isEmpty) {
          rdd.foreach(println);
        }
      }
      //      lines.foreachRDD { rdd =>
      //        rdd.foreach(CR => println(CR.key() + "," + CR.value()));
      //      }

      ssc.start();
      ssc.awaitTermination()

    }

}