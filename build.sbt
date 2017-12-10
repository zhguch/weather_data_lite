name :="Weather_Data_Lite"
version :="1.0"
scalaVersion :="2.11.8"

val sparkVersion = "2.2.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"
)

fork in run := true

javaOptions in run ++= Seq(
    "-Xms4G", "-Xmx8G", "-XX:MaxPermSize=4G", "-XX:+UseConcMarkSweepGC")
