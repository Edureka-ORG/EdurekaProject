package com.edureka.loanrepayment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object LoanRepaymentAnalysis {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf();
    sparkConf.setAppName("Loan-Repayment-analysis");
    //    sparkConf.setMaster("yarn);
    //    sparkConf.set("spark.submit.deployMode", "client");

    val spark = SparkSession.builder().config(sparkConf).getOrCreate();

    //Load and Parse the Dataset
    val sqoopLoc = args(0); //"hdfs:/user/edureka_144865/edurekaproject/sqoop/*"
    val raw = spark.read.text(sqoopLoc);

    import spark.implicits._;

    val df = raw.map(_.getString(0).split(","))
      .map(
        d => Data(
          d(0).toInt,
          d(1).toString,
          d(2).toString,
          d(3).toString,
          d(4).toString,
          d(5).toInt,
          d(6).toDouble,
          d(7).toDouble,
          d(8).toDouble,
          d(9).toString)).toDF

    //    Create new column

    import org.apache.spark.sql.functions._
    val df1 = df.withColumn("CREDIT_INCOME_PERCENT", col("AMT_CREDIT") / col("AMT_INCOME_TOTAL"))

    df1 show 1

    //Load and Parse the Dataset using DataFrames

    val columns = Seq(
      "TARGET", "NAME_CONTRACT_TYPE", "CODE_GENDER", "FLAG_OWN_CAR",
      "FLAG_OWN_REALTY", "CNT_CHILDREN", "AMT_INCOME_TOTAL", "AMT_CREDIT",
      "AMT_ANNUITY", "NAME_INCOME_TYPE", "NAME_EDUCATION_TYPE",
      "NAME_FAMILY_STATUS", "NAME_HOUSING_TYPE", "DAYS_BIRTH",
      "DAYS_EMPLOYED", "FLAG_MOBIL", "FLAG_EMP_PHONE", "FLAG_WORK_PHONE",
      "FLAG_CONT_MOBILE", "FLAG_PHONE", "CNT_FAM_MEMBERS",
      "REGION_RATING_CLIENT", "REGION_RATING_CLIENT_W_CITY",
      "REG_REGION_NOT_LIVE_REGION", "REG_REGION_NOT_WORK_REGION",
      "ORGANIZATION_TYPE", "FLAG_DOCUMENT_2", "FLAG_DOCUMENT_3",
      "FLAG_DOCUMENT_4", "FLAG_DOCUMENT_5", "FLAG_DOCUMENT_6", "FLAG_DOCUMENT_7",
      "FLAG_DOCUMENT_8", "FLAG_DOCUMENT_9", "FLAG_DOCUMENT_10",
      "FLAG_DOCUMENT_11", "FLAG_DOCUMENT_12");

    val data = spark.read.option("inferSchema", true).csv(sqoopLoc).limit(1000).toDF(columns: _*)

    //Cache datatset
    data.cache()

    //Exploratory Analysis
    //No of loans falling into each Target with percentage

    import org.apache.spark.sql.functions._
    data.groupBy("TARGET").count().withColumn("Percentage", col("count") * 100 / data.count()).show()

    //Number of missing values in each column

    val nullcounts = data.select(data.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*)
    val totcount = data.count
    nullcounts.first().toSeq.zip(data.columns).map(x => (
      x._1,
      "%1.2f".format(x._1.toString.toDouble * 100 / totcount), x._2)).foreach(println)

    //View unique values in all string columns

    import org.apache.spark.sql.types._
    val exprs = data.schema.fields.filter(x => x.dataType == StringType)
      .map(x => x.name -> "approx_count_distinct").toMap

    data.agg(exprs).show()

    //    Describe days employed
    data.select("DAYS_EMPLOYED").describe().show()

    //    Describe days birth column
    val dfAge = data.withColumn("AGE", col("DAYS_BIRTH") / (-365))

    dfAge.select("DAYS_BIRTH", "AGE").describe().show()

    //    Dig deep into anomalies of DAY_EMPLOYED column
    val anom = dfAge.filter(col("DAYS_EMPLOYED").equalTo(365243))
    val non_anom = dfAge.filter(col("DAYS_EMPLOYED").notEqual(365243))

    val nonanomPer = 100 * non_anom.agg(avg(col("TARGET"))).first()(0).toString.toDouble
    val anomPer = 100 * anom.agg(avg(col("TARGET"))).first()(0).toString.toDouble
    println(f"The non-anomalies default on $nonanomPer%2.2f while anomalies default on$anomPer%2.2f ")

    val anomCount = anom.count
    print(f"There are $anomCount%d anomalous days of employment"); // no of wrong employment day column

    //Create anomaly flag column

    val anomalyDf = dfAge.withColumn("DAYS_EMPLOYED_ANOM", col("DAYS_EMPLOYED").equalTo(365243))

    //Replace anomaly value with 0

    val anomalyFlagDf = anomalyDf.withColumn("DAYS_EMPLOYED", when(col("DAYS_EMPLOYED") === 365243, 0).otherwise(col("DAYS_EMPLOYED"))) // if anom is 365243 convert to 0

    //Effect of age on repayment by binning the column and the generating pivot table

    anomalyFlagDf.select("AGE").describe().show()

    //Create new variables based on domain knowledge

    val tmpDf1 = anomalyFlagDf.withColumn("CREDIT_INCOME_PERCENT", col("AMT_CREDIT") / col("AMT_INCOME_TOTAL"))
    val tmpDf2 = tmpDf1.withColumn("ANNUITY_INCOME_PERCENT", col("AMT_ANNUITY") / col("AMT_INCOME_TOTAL"))
    val tmpDf3 = tmpDf2.withColumn("CREDIT_TERM", col("AMT_ANNUITY") / col("AMT_CREDIT"))
    val tmpDf4 = tmpDf3.withColumn("DAYS_EMPLOYED_PERCENT", col("DAYS_EMPLOYED") / col("DAYS_BIRTH"))
    val newDf = tmpDf4.withColumn("label", col("TARGET"))

    //NOTE: Convert string column with only 2 unique values to a column of label indices to make the values readable for machine learning algorithm

    //ONE WAY IS BY DOING IT MANUALLY

    import org.apache.spark.ml.feature.StringIndexer

    val indexer = new StringIndexer().setInputCol("NAME_CONTRACT_TYPE").setOutputCol("NAME_CONTRACT_TYPE_Index")
    val indexed = indexer.fit(newDf).transform(newDf)

    //SMARTER WAY WILL BE TO DO THIS USING PIPELINE

    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.feature.StringIndexer
    val indexers =
      Array("NAME_CONTRACT_TYPE", "CODE_GENDER", "FLAG_OWN_CAR", "FLAG_OWN_REALTY")
        .map(c => new StringIndexer().setInputCol(c).setOutputCol(c + "_Index"))
    val pipeline = new Pipeline().setStages(indexers)
    val df_r = pipeline.fit(newDf).transform(newDf)

    //Convert string column with values > 2 to onehotencoder

    //MANUALLY:

    import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer }
    val indexer1 = new StringIndexer().setInputCol("NAME_INCOME_TYPE").setOutputCol("categoryIndex").fit(df_r)
    val indexed1 = indexer1.transform(df_r)
    val encoder = new OneHotEncoder().setInputCol("NAME_CONTRACT_TYPE_Index").setOutputCol("categoryVec")
    val encoded = encoder.transform(indexed1)

    //THROUGH PIPELINE:

    import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer }
    val indexers1 = Array("NAME_INCOME_TYPE", "NAME_EDUCATION_TYPE", "ORGANIZATION_TYPE").map(c => new StringIndexer().setInputCol(c).setOutputCol(c + "_Index"))
    val encoder1 = Array("NAME_INCOME_TYPE", "NAME_EDUCATION_TYPE", "ORGANIZATION_TYPE").map(column => new OneHotEncoder().setInputCol(column + "_Index").setOutputCol(column + "_Vec"))
    val encoderPipeline = new Pipeline().setStages(indexers1 ++ encoder1)
    val encoded1 = encoderPipeline.fit(df_r).transform(df_r)
    encoded1.show(1)

    //Convert AGE column to bins (converting age in four categories)

    import org.apache.spark.ml.feature.Bucketizer
    val splits = Array(0, 25.0, 35.0, 55.0, 100.0)
    val bucketizer = new Bucketizer().setInputCol("AGE").setOutputCol("bucketedData").setSplits(splits)
    val bucketedData = bucketizer.transform(encoded)
    bucketedData.groupBy("bucketedData").pivot("TARGET").count().show() // bucketeddata is output column name

    //Generate feature columns (discarded string only index columns)

    val feature_cols =
      Array("CNT_CHILDREN", "AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY", "DAYS_EMPLOYED",
        "FLAG_MOBIL", "FLAG_EMP_PHONE", "FLAG_WORK_PHONE", "FLAG_CONT_MOBILE", "FLAG_PHONE",
        "CNT_FAM_MEMBERS", "REGION_RATING_CLIENT", "REGION_RATING_CLIENT_W_CITY", "REG_REGION_NOT_LIVE_REGION",
        "REG_REGION_NOT_WORK_REGION", "FLAG_DOCUMENT_2", "FLAG_DOCUMENT_3", "FLAG_DOCUMENT_4",
        "FLAG_DOCUMENT_5", "FLAG_DOCUMENT_6", "FLAG_DOCUMENT_7",
        "FLAG_DOCUMENT_8", "FLAG_DOCUMENT_9", "FLAG_DOCUMENT_10", "FLAG_DOCUMENT_11",
        "FLAG_DOCUMENT_12", "NAME_CONTRACT_TYPE_Index", "CODE_GENDER_Index",
        "FLAG_OWN_CAR_Index", "FLAG_OWN_REALTY_Index", "NAME_INCOME_TYPE_Vec", "NAME_EDUCATION_TYPE_Vec",
        "ORGANIZATION_TYPE_Vec", "AGE", "DAYS_EMPLOYED_ANOM", "bucketedData", "CREDIT_INCOME_PERCENT",
        "ANNUITY_INCOME_PERCENT", "CREDIT_TERM", "DAYS_EMPLOYED_PERCENT")

    //Assemble features (assemble all features in one vector)

    import org.apache.spark.ml.feature.VectorAssembler
    val assembler = new VectorAssembler().setInputCols(feature_cols).setOutputCol("features")
    val output = assembler.transform(bucketedData)

    //Train logistic Regression model (creating initializing and fitting model)

    import org.apache.spark.ml.classification.LogisticRegression
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lrModel = lr.fit(output)
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    //Get model Accuracy

    import org.apache.spark.sql.types._

    import org.apache.spark.mllib.evaluation.MulticlassMetrics

    val transformed = lrModel.transform(output)
    val results = transformed.select("prediction", "label").withColumn(
      "label",
      col("label").cast(DoubleType))

    val predictionAndLabels = results.rdd.map(row => (
      row(0).toString.toDouble,
      row(1).toString.toDouble))

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println("Confusion matrix:")
    println(metrics.confusionMatrix)
    val accuracy = metrics.accuracy
    println("Summary Statistics")
    println(s"Accuracy = $accuracy")
    
    
    //Generating Pipeline
    // scalastyle:off println

  }
}