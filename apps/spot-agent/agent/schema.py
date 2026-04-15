OUTPUT_SCHEMA = {
  "type": "object",
  "properties": {
    "report_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
    "provinces": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "name_cn": {"type": "string"},
          "hourly": {"type": "array", "minItems": 24, "maxItems": 24, "items": {"type": "number"}},
          "daily_summary": {
            "type": "object",
            "properties": {
              "rt_avg": {"type": "number"},
              "rt_max": {"type": "number"},
              "rt_min": {"type": "number"},
              "da_avg": {"type": "number"},
              "spread": {"type": "number"}
            },
            "required": ["rt_avg","rt_max","rt_min"]
          },
          "validation": {
            "type": "object",
            "properties": {
              "rmse": {"type": "number"},
              "within_tolerance": {"type": "boolean"},
              "notes": {"type": "string"}
            },
            "required": ["within_tolerance"]
          },
          "provenance": {
            "type": "object",
            "properties": {
              "pdf_path": {"type": "string"},
              "page": {"type": "integer"},
              "box": {"type": "array", "items": {"type": "number"}}
            }
          }
        },
        "required": ["name_cn","hourly","daily_summary","validation"]
      }
    }
  },
  "required": ["report_date","provinces"]
}
