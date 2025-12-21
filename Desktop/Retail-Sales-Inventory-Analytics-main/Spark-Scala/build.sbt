name := "spark-scala"

version := "0.1"


scalaVersion := "2.12.18"


val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,


  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,



  "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0"
)