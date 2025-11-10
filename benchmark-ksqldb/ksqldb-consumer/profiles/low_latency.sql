-- Drop existing table if exists
DROP TABLE IF EXISTS weather_aggregated_output DELETE TOPIC;

-- Create aggregation with 30-second windows
CREATE TABLE weather_aggregated_output WITH (
  KAFKA_TOPIC='weather.aggregated.output',
  VALUE_FORMAT='AVRO',
  PARTITIONS=5,
  REPLICAS=1
) AS
SELECT
  stationId AS stationId,
  LATEST_BY_OFFSET(stationName) AS stationName,
  AS_VALUE(stationId) AS key_stationId,
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_end,
  metric AS metric,
  ROUND(AVG(value), 2) AS avg_value,
  ROUND(MIN(value), 2) AS min_value,
  ROUND(MAX(value), 2) AS max_value,
  COUNT(*) AS message_count,
  MIN(producer_ts) AS min_producer_ts,
  MAX(ROWTIME) AS processing_end_ts
FROM (
  SELECT * FROM weather_wind
  UNION ALL
  SELECT * FROM weather_sunshine
) WINDOW TUMBLING (SIZE 30 SECONDS)
GROUP BY stationId, metric
EMIT CHANGES;