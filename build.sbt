name := "SamplePHData"
version := "0.1"
val sparkVersion = "2.2.0"
scalaVersion := "2.11.11"

libraryDependencies ++=  Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.0.0"
)

dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.7"

