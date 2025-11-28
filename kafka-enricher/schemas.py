# schemas.py
# Avro schemas for enriched weather data output

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
    {"name": "DkArea", "type": ["null", "string"], "default": null},
    {"name": "MunicipalityCode", "type": ["null", "string"], "default": null}
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
    {"name": "DkArea", "type": ["null", "string"], "default": null},
    {"name": "MunicipalityCode", "type": ["null", "string"], "default": null}
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
    {"name": "DkArea", "type": ["null", "string"], "default": null},
    {"name": "MunicipalityCode", "type": ["null", "string"], "default": null}
  ]
}
"""