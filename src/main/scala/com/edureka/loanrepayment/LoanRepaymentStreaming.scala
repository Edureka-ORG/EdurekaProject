package com.edureka.loanrepayment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.Encoders
import org.apache.spark.ml._

import org.apache.spark.streaming.kafka._

object LoanRepaymentStreaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LoanRepaymentStreaming")

    val ssc = new StreamingContext(conf, Seconds(10))
    val topicMap = "edurekaproject".split(",").map((_, 1)).toMap

    val lines = KafkaUtils.createStream(
      ssc,
      "ip-20-0-21-161.ec2.internal:2181 ",
      "spark-streamingconsumer",
      topicMap).map(_._2)

    //val lines = ssc.textFileStream("tmp/kafka/spam_message")

    lines.foreachRDD { rdd =>
      if (!rdd.isEmpty) {
        println("=====================================")
        val spark = SparkSession.builder().getOrCreate()

        import spark.implicits._

        val rawRdd = rdd.map(_.split(",")).map(d => Data1(d(0).toString, d(1).toString, d(2).toString,
          d(3).toString, d(4).toInt, d(5).toDouble, d(6).toDouble, d(7).toDouble, d(8).toString, d(9).toString,
          d(10).toString, d(11).toString, d(12).toInt, d(13).toInt, d(14).toInt, d(15).toInt, d(16).toInt,
          d(17).toInt, d(18).toInt, d(19).toDouble, d(20).toInt, d(21).toInt, d(22).toInt, d(23).toInt,
          d(24).toString, d(25).toInt, d(26).toInt, d(27).toInt, d(28).toInt, d(29).toInt, d(30).toInt, d(31).toInt,
          d(32).toInt, d(33).toInt, d(34).toInt, d(35).toInt))

        val raw = spark.createDataFrame(rawRdd)
        // Add age columns
        val dfAge = raw.withColumn("AGE", col("DAYS_BIRTH") / (-365))
        // Add anomaly flag and replace it with 0

        val anomalyFlagDf = dfAge.withColumn("DAYS_EMPLOYED_ANOM", col("DAYS_EMPLOYED").equalTo(365243))
        val anomalyDf = anomalyFlagDf.withColumn("DAYS_EMPLOYED", when(col("DAYS_EMPLOYED") === 365243, 0).otherwise(col("DAYS_EMPLOYED")))

        // Rename column TARGET to label
        val labelDf = anomalyDf //.withColumn("label",col("TARGET"))

        // create domain features
        val tmpDf1 = labelDf.withColumn("CREDIT_INCOME_PERCENT", col("AMT_CREDIT") / col("AMT_INCOME_TOTAL"))

        val tmpDf2 = tmpDf1.withColumn("ANNUITY_INCOME_PERCENT", col("AMT_ANNUITY") / col("AMT_INCOME_TOTAL"))
        val tmpDf3 = tmpDf2.withColumn("CREDIT_TERM", col("AMT_ANNUITY") / col("AMT_CREDIT"))
        val df = tmpDf3.withColumn("DAYS_EMPLOYED_PERCENT", col("DAYS_EMPLOYED") / col("DAYS_BIRTH"))

        // define columns that will be used as feature variables in model training
        val feature_cols = Array(
          "CNT_CHILDREN",
          "AMT_INCOME_TOTAL",
          "AMT_CREDIT",
          "AMT_ANNUITY",
          "DAYS_EMPLOYED",
          "FLAG_MOBIL",
          "FLAG_EMP_PHONE",
          "FLAG_WORK_PHONE",
          "FLAG_CONT_MOBILE",
          "FLAG_PHONE",
          "CNT_FAM_MEMBERS",
          "REGION_RATING_CLIENT",
          "REGION_RATING_CLIENT_W_CITY",
          "REG_REGION_NOT_LIVE_REGION",
          "REG_REGION_NOT_WORK_REGION",
          "FLAG_DOCUMENT_2",
          "FLAG_DOCUMENT_3",
          "FLAG_DOCUMENT_4",
          "FLAG_DOCUMENT_5",
          "FLAG_DOCUMENT_6",
          "FLAG_DOCUMENT_7",
          "FLAG_DOCUMENT_8",
          "FLAG_DOCUMENT_9",
          "FLAG_DOCUMENT_10",
          "FLAG_DOCUMENT_11",
          "FLAG_DOCUMENT_12",
          "NAME_CONTRACT_TYPE_Index",
          "CODE_GENDER_Index",
          "FLAG_OWN_CAR_Index",
          "FLAG_OWN_REALTY_Index",
          "NAME_INCOME_TYPE_Vec",
          "NAME_EDUCATION_TYPE_Vec",
          "ORGANIZATION_TYPE_Vec",
          "AGE",
          "DAYS_EMPLOYED_ANOM",
          "bucketedData",
          "CREDIT_INCOME_PERCENT",
          "ANNUITY_INCOME_PERCENT",
          "CREDIT_TERM",
          "DAYS_EMPLOYED_PERCENT")

        val pipeline = PipelineModel.read.load("inclass/scalamodel.model")
        val predictions = pipeline.transform(df)
        println("==========================")
        println(predictions.show(1))

      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}