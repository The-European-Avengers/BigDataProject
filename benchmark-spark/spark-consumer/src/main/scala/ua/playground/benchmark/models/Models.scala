package ua.playground.benchmark.models

/**
 * Shared models for Weather Streaming Benchmark
 * These models are used by the Consumer to work with data structures
 */

/** Input weather data from Kafka */
case class WeatherData(
                        timeObserved: String,
                        stationId: Int,
                        stationName: String,
                        metric: String,        // "wind_speed" or "sunshine"
                        value: Double,
                        producer_ts: Long      // Producer timestamp for latency calculation
                      )

/** Aggregated weather data (output) */
case class WeatherAggregation(
                               windowStart: String,
                               windowEnd: String,
                               metric: String,
                               stationId: Int,
                               stationName: String,
                               avgValue: Double,
                               minValue: Double,
                               maxValue: Double,
                               messageCount: Long,
                               minProducerTs: Long,       // Earliest producer timestamp in window
                               processingEndTs: Long      // When aggregation completed
                             )