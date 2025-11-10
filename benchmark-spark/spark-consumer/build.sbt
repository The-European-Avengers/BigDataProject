ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.7"

val sparkVersion = "3.5.1"
val kafkaVersion = "3.6.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-consumer",

    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" % "spark-sql_2.13" % sparkVersion,
      "org.apache.spark" % "spark-streaming_2.13" % sparkVersion,
      "org.apache.spark" % "spark-sql-kafka-0-10_2.13" % sparkVersion,
      "org.apache.spark" % "spark-avro_2.13" % sparkVersion,

      // Kafka
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,

      // Avro
      "org.apache.avro" % "avro" % "1.11.3",
      "io.confluent" % "kafka-avro-serializer" % "7.5.1",

      // Config
      "com.typesafe" % "config" % "1.4.3",

      // Logging
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",
      "ch.qos.logback" % "logback-classic" % "1.4.14"
    ),

    resolvers += "Confluent" at "https://packages.confluent.io/maven/",

    fork := true,

    // ✅ ADD THIS - Specify main class
    assembly / mainClass := Some("ua.playground.benchmark.SparkWeatherConsumer"),

    // Assembly settings
    assembly / assemblyJarName := "spark-consumer.jar",

    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(true),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case "application.conf" => MergeStrategy.concat
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.contains("hadoop") => MergeStrategy.first
      case x => MergeStrategy.first
    }
  )