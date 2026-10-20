# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Extract structure from unstructured text and documents
# MAGIC
# MAGIC Genie answers questions with SQL. Free text and PDFs become queryable once the facts inside them are
# MAGIC extracted into columns. This notebook uses Databricks AI Functions to do that in SQL:
# MAGIC
# MAGIC | Source | Function | Output table | Extracted fields |
# MAGIC |---|---|---|---|
# MAGIC | Complaint narratives | `ai_extract` | `complaints_extracted` | failure mode, patient injury, injury description, lot number |
# MAGIC | Technician notes | `ai_extract` | `service_findings` | failure confirmed, defect observed, part replaced |
# MAGIC | CAPA PDFs in a volume | `ai_parse_document` → `ai_extract` | `capa_documents` | CAPA number, status, component, supplier, affected lots, root cause, risk |
# MAGIC
# MAGIC The original text is kept next to every extracted value so a reviewer can always trace a number back to its source.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "pms_genie_cowork")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")


def lit(text: str) -> str:
    """Quote a Python string as a SQL string literal."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"

# COMMAND ----------

# MAGIC %md
# MAGIC `ai_extract` returns a VARIANT. Depending on the function version, each field is either a plain value or an object
# MAGIC `{"value": ...}` with optional citations and confidence. `field()` reads either shape.

# COMMAND ----------

def field(name: str, sql_type: str, src: str = "extraction") -> str:
    """SQL expression that reads one extracted field from an ai_extract result.

    Newer function versions wrap each value as {"value": ...}; older ones return the value directly.
    A wrapped null stays null instead of becoming the text '{"value":null}'.
    """
    raw = f"try_variant_get({src}, '$.response.{name}', 'variant')"
    if sql_type.startswith("array"):
        return (f"transform(try_variant_get({src}, '$.response.{name}', 'array<variant>'), "
                f"v -> CASE WHEN schema_of_variant(v) LIKE 'OBJECT%' THEN try_variant_get(v, '$.value', 'string') "
                f"ELSE try_variant_get(v, '$', 'string') END)")
    return (f"CASE WHEN {raw} IS NULL THEN NULL "
            f"WHEN schema_of_variant({raw}) LIKE 'OBJECT%' THEN try_variant_get({src}, '$.response.{name}.value', '{sql_type}') "
            f"ELSE try_variant_get({src}, '$.response.{name}', '{sql_type}') END")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Complaint narratives → structured fields

# COMMAND ----------

complaint_schema = """{
  "failure_mode": {"type": "enum", "labels": ["cuff_deflation_failure", "inaccurate_reading", "display_error",
                   "battery_or_power", "compressor_failure", "physical_damage", "connectivity", "other"]},
  "patient_injury": {"type": "boolean"},
  "injury_description": {"type": "string"},
  "lot_number": {"type": "string"}
}"""

complaint_instructions = (
    "These are medical device complaint narratives taken at intake. "
    "failure_mode: cuff_deflation_failure means a blood pressure cuff that stays inflated or deflates slowly; "
    "battery_or_power covers batteries, battery doors and unexpected power-off; "
    "connectivity covers app pairing. "
    "patient_injury is true only when the text describes harm to a person such as bruising, pain, numbness or dizziness. "
    "injury_description is a short phrase describing the harm, or null. "
    "lot_number is the manufacturing lot printed on the device, format like BP3-2605-A, or null when not stated."
)

spark.sql(f"""
CREATE OR REPLACE TABLE complaints_extracted
COMMENT 'Complaint records with fields extracted from the free-text narrative by ai_extract. Synthetic.'
AS
WITH x AS (
  SELECT *,
         ai_extract(narrative, {lit(complaint_schema)}, map('instructions', {lit(complaint_instructions)})) AS extraction
  FROM complaints_raw
)
SELECT complaint_id, received_date, family_id,
       lot_id AS lot_id_in_record,
       intake_channel, country, region, reporter_contact, narrative,
       {field('failure_mode', 'string')}        AS failure_mode,
       {field('patient_injury', 'boolean')}     AS patient_injury,
       {field('injury_description', 'string')}  AS injury_description,
       upper({field('lot_number', 'string')})   AS lot_id_from_narrative,
       try_variant_get(extraction, '$.error_message', 'string')                AS extraction_error
FROM x
""")

display(spark.sql("""
SELECT failure_mode, count(*) AS complaints, sum(CASE WHEN patient_injury THEN 1 ELSE 0 END) AS with_injury
FROM complaints_extracted GROUP BY ALL ORDER BY complaints DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technician notes → structured fields

# COMMAND ----------

service_schema = """{
  "failure_confirmed": {"type": "boolean"},
  "observed_defect": {"type": "string"},
  "part_replaced": {"type": "string"}
}"""
service_instructions = (
    "These are service technician notes for returned medical devices. "
    "failure_confirmed is true when the technician reproduced or found a defect, false for no fault found. "
    "observed_defect is a short phrase. part_replaced is the part name or part number replaced, or null."
)

spark.sql(f"""
CREATE OR REPLACE TABLE service_findings
COMMENT 'Returned-unit evaluations with fields extracted from technician notes by ai_extract. Synthetic.'
AS
WITH x AS (
  SELECT *, ai_extract(technician_notes, {lit(service_schema)}, map('instructions', {lit(service_instructions)})) AS extraction
  FROM service_tickets_raw
)
SELECT ticket_id, complaint_id, evaluated_date, family_id, lot_id, technician_notes, resolution_code,
       {field('failure_confirmed', 'boolean')} AS failure_confirmed,
       {field('observed_defect', 'string')}    AS observed_defect,
       {field('part_replaced', 'string')}      AS part_replaced
FROM x
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CAPA PDFs → parsed text → structured fields
# MAGIC
# MAGIC `ai_parse_document` reads each PDF from the volume and returns its layout elements. The text is concatenated
# MAGIC and passed to `ai_extract`. The full document text stays in the table so Genie can quote it.

# COMMAND ----------

capa_schema = """{
  "capa_number": {"type": "string"},
  "date_opened": {"type": "string"},
  "status": {"type": "string"},
  "device_family": {"type": "string"},
  "component": {"type": "string"},
  "supplier": {"type": "string"},
  "affected_lots": {"type": "array", "items": {"type": "string"}},
  "root_cause": {"type": "string"},
  "risk_rating": {"type": "enum", "labels": ["High", "Medium", "Low"]}
}"""

spark.sql(f"""
CREATE OR REPLACE TABLE capa_documents
COMMENT 'CAPA investigation reports parsed from PDF with ai_parse_document and structured with ai_extract. Synthetic.'
AS
WITH parsed AS (
  SELECT path, ai_parse_document(content, map('version', '2.0', 'descriptionElementTypes', '')) AS doc
  FROM read_files('/Volumes/{catalog}/{schema}/documents/capa/', format => 'binaryFile')
),
text AS (
  SELECT path, concat_ws('\\n', transform(CAST(doc:document:elements AS ARRAY<VARIANT>), e -> e:content::STRING)) AS document_text
  FROM parsed
),
x AS (
  SELECT path, document_text,
         ai_extract(document_text, {lit(capa_schema)},
                    map('instructions', 'This is a CAPA report for a medical device. date_opened as YYYY-MM-DD. '
                                        'affected_lots lists lot numbers only.')) AS extraction
  FROM text
)
SELECT {field('capa_number', 'string')}          AS capa_number,
       try_cast({field('date_opened', 'string')} AS DATE) AS date_opened,
       {field('status', 'string')}               AS status,
       {field('device_family', 'string')}        AS device_family,
       {field('component', 'string')}            AS component,
       {field('supplier', 'string')}             AS supplier,
       {field('affected_lots', 'array<string>')} AS affected_lots,
       {field('root_cause', 'string')}           AS root_cause,
       {field('risk_rating', 'string')}          AS risk_rating,
       document_text,
       path AS source_file
FROM x
""")

display(spark.sql("SELECT capa_number, date_opened, status, component, supplier, affected_lots, risk_rating FROM capa_documents ORDER BY date_opened"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality gate
# MAGIC
# MAGIC The run stops here if extraction failed, so later steps never build on incomplete data.

# COMMAND ----------

checks = spark.sql("""
SELECT
  (SELECT count(*) FROM complaints_raw)                                        AS complaints_in,
  (SELECT count(*) FROM complaints_extracted)                                  AS complaints_out,
  (SELECT count_if(failure_mode IS NULL) FROM complaints_extracted)            AS missing_failure_mode,
  (SELECT count_if(extraction_error IS NOT NULL) FROM complaints_extracted)    AS extraction_errors,
  (SELECT count(*) FROM capa_documents WHERE capa_number IS NOT NULL)          AS capa_parsed,
  (SELECT count_if(lot_id_from_narrative LIKE '{%') FROM complaints_extracted) AS unparsed_values
""").first()
print(checks.asDict())
assert checks.complaints_out == checks.complaints_in, "Some complaints were dropped during extraction."
assert checks.extraction_errors == 0, "ai_extract returned errors; inspect complaints_extracted.extraction_error."
assert checks.missing_failure_mode <= 0.02 * checks.complaints_in, "More than 2% of complaints have no failure mode."
assert checks.capa_parsed == 4, "Expected 4 parsed CAPA documents."
assert checks.unparsed_values == 0, "Some extracted values were not unwrapped; check field()."
