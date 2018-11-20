import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.8",
      version      := "1.0"
    )),
    name := "EdurekaProject",
    libraryDependencies += scalaTest % Test
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.11.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.1" % "runtime"

