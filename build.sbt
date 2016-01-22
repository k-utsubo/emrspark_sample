name := "spark_sample"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"


// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-sql" % "1.5.2",
  "org.apache.spark" %% "spark-hive" % "1.5.2",
  "org.apache.spark" %% "spark-mllib" % "1.5.2",
   "com.amazonaws" % "aws-java-sdk" % "1.10.46"
)


