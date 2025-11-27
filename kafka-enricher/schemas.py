# schemas.py
# Avro schema strings for enriched output topics.
# These schemas match the outgoing enriched record structure.

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
    {"name": "DkArea", "type": ["null", "string"], "default": null},
    {"name": "MunicipalityCode", "type": ["null", "string"], "default": null}
  ]
}
"""
