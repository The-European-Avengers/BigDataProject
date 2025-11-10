-- Create source streams from Kafka topics with Avro schema
CREATE STREAM weather_wind (
  timeObserved VARCHAR,
  stationId INT,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.wind',
  VALUE_FORMAT='AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME='ua.playground.benchmark.WeatherData'
);

CREATE STREAM weather_sunshine (
  timeObserved VARCHAR,
  stationId INT,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.sunshine',
  VALUE_FORMAT='AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME='ua.playground.benchmark.WeatherData'
);