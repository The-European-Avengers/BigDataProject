# schemas.py
# Avro schemas for enriched weather data output (WITH forecastId)

WIND_ENRICHED_SCHEMA = """
{
  "type": "record",
  "name": "WindEnriched",
  "namespace": "weather.enriched",
  "fields": [
    {"name": "lon", "type": "double"},
    {"name": "lat", "type": "double"},
    {"name": "value", "type": "double"},
    {"name": "step", "type": "string"},
    {"name": "parameter", "type": "string"},
    {"name": "forecastId", "type": "string"},
    {"name": "dkArea", "type": "int"},
    {"name": "municipalityCode", "type": "int"}
  ]
}
"""

TEMP_ENRICHED_SCHEMA = """
{
  "type": "record",
  "name": "TempEnriched",
  "namespace": "weather.enriched",
  "fields": [
    {"name": "lon", "type": "double"},
    {"name": "lat", "type": "double"},
    {"name": "value", "type": "double"},
    {"name": "step", "type": "string"},
    {"name": "parameter", "type": "string"},
    {"name": "forecastId", "type": "string"},
    {"name": "dkArea", "type": "int"},
    {"name": "municipalityCode", "type": "int"}
  ]
}
"""

SUN_ENRICHED_SCHEMA = """
{
  "type": "record",
  "name": "SunEnriched",
  "namespace": "weather.enriched",
  "fields": [
    {"name": "lon", "type": "double"},
    {"name": "lat", "type": "double"},
    {"name": "value", "type": "double"},
    {"name": "step", "type": "string"},
    {"name": "parameter", "type": "string"},
    {"name": "forecastId", "type": "string"},
    {"name": "dkArea", "type": "int"},
    {"name": "municipalityCode", "type": "int"}
  ]
}
"""