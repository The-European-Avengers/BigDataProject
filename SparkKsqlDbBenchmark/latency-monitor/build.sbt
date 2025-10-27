name := "latency-monitor"
version := "1.0.0"
scalaVersion := "3.3.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.5.1",
  "org.apache.avro" % "avro" % "1.11.3",
  "io.confluent" % "kafka-avro-serializer" % "7.5.0",
  "ch.qos.logback" % "logback-classic" % "1.4.11",
  "com.typesafe" % "config" % "1.4.2"
)

resolvers += "Confluent" at "https://packages.confluent.io/maven/"

// Assembly settings
assembly / assemblyJarName := "latency-monitor.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}