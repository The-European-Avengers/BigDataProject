name := "producer"
version := "1.0.0"
scalaVersion := "3.3.7"

libraryDependencies ++= Seq(
  // Kafka
  "org.apache.kafka" % "kafka-clients" % "3.5.1",

  // Avro
  "org.apache.avro" % "avro" % "1.11.3",
  "io.confluent" % "kafka-avro-serializer" % "7.5.0",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.11"
)

resolvers += "Confluent" at "https://packages.confluent.io/maven/"

// ✅ ADD THIS - Specify main class
assembly / mainClass := Some("ua.playground.benchmark.BenchmarkRunner")

// Assembly settings
assembly / assemblyJarName := "benchmark-producer.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}