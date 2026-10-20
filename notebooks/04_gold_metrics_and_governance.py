# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold tables, metric views, and governance
# MAGIC
# MAGIC This notebook turns the extracted data into the governed layer Genie works from.
# MAGIC
# MAGIC 1. **`complaints_enriched`**: one row per complaint. The lot is resolved from the structured record or, when blank,
# MAGIC    from the narrative, then joined to manufacturing and supplier data. `lot_source` records where the lot came from.
# MAGIC 2. **`monthly_quality`** and **`lot_quality`**: aggregates that pair complaint counts with units shipped.
# MAGIC 3. **Metric views** `pms_monthly_metrics` and `pms_lot_metrics`: one certified definition of complaint rate,
# MAGIC    so every user and every Genie answer computes it the same way.
# MAGIC 4. **Governance**: column comments, table tags, and a column mask on reporter contact details.
# MAGIC
# MAGIC Metric definitions in this demo are illustrative. A real quality system uses the definitions in its own procedures.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "pms_genie_cowork")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
fq = f"`{catalog}`.`{schema}`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complaint-level gold table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE complaints_enriched
COMMENT 'One row per complaint with extracted failure mode and injury, resolved lot, supplier and service outcome. Synthetic.'
AS
SELECT c.complaint_id,
       c.received_date,
       date_trunc('MONTH', c.received_date)                         AS received_month,
       c.family_id,
       f.family_name,
       coalesce(c.lot_id_in_record, c.lot_id_from_narrative)        AS lot_id,
       CASE WHEN c.lot_id_in_record IS NOT NULL THEN 'intake record'
            WHEN c.lot_id_from_narrative IS NOT NULL THEN 'extracted from narrative'
            ELSE 'unknown' END                                      AS lot_source,
       l.build_date                                                 AS lot_build_date,
       l.site                                                       AS manufacturing_site,
       s.supplier_name                                              AS key_component_supplier,
       f.key_component,
       c.failure_mode,
       coalesce(c.patient_injury, false)                            AS patient_injury,
       c.injury_description,
       c.intake_channel,
       c.country,
       c.region,
       c.reporter_contact,
       sf.ticket_id                                                 AS service_ticket_id,
       sf.failure_confirmed                                         AS service_failure_confirmed,
       sf.part_replaced                                             AS service_part_replaced,
       c.narrative
FROM complaints_extracted c
JOIN device_families f       ON c.family_id = f.family_id
LEFT JOIN manufacturing_lots l ON coalesce(c.lot_id_in_record, c.lot_id_from_narrative) = l.lot_id
LEFT JOIN suppliers s        ON l.supplier_id = s.supplier_id
LEFT JOIN service_findings sf ON c.complaint_id = sf.complaint_id
""")

display(spark.sql("SELECT lot_source, count(*) AS complaints FROM complaints_enriched GROUP BY ALL"))

coverage = spark.sql("""
SELECT count(*) AS complaints,
       count_if(lot_id IS NOT NULL) AS with_lot,
       count_if(lot_id IS NOT NULL AND lot_build_date IS NULL) AS unmatched_lot
FROM complaints_enriched
""").first()
print(coverage.asDict())
assert coverage.unmatched_lot == 0, "Some extracted lot numbers do not match a manufacturing lot."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregates with a units-shipped denominator
# MAGIC
# MAGIC A metric view reads from one source table, so complaint counts and units shipped are joined into
# MAGIC one table per grain before the metric view is defined.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE monthly_quality
COMMENT 'Complaints and units shipped per device family, month and region. Synthetic.'
AS
WITH shipped AS (
  SELECT family_id, ship_month AS month, region, sum(units_shipped) AS units_shipped
  FROM shipments GROUP BY ALL
),
complained AS (
  SELECT family_id, received_month AS month, region,
         count(*)                                         AS complaints,
         count_if(patient_injury)                         AS injury_complaints,
         count_if(failure_mode = 'cuff_deflation_failure') AS cuff_deflation_complaints
  FROM complaints_enriched GROUP BY ALL
)
SELECT f.family_id, f.family_name, coalesce(s.month, c.month) AS month, coalesce(s.region, c.region) AS region,
       coalesce(s.units_shipped, 0)             AS units_shipped,
       coalesce(c.complaints, 0)                AS complaints,
       coalesce(c.injury_complaints, 0)         AS injury_complaints,
       coalesce(c.cuff_deflation_complaints, 0) AS cuff_deflation_complaints
FROM shipped s
FULL OUTER JOIN complained c ON s.family_id = c.family_id AND s.month = c.month AND s.region = c.region
JOIN device_families f ON f.family_id = coalesce(s.family_id, c.family_id)
""")

spark.sql("""
CREATE OR REPLACE TABLE lot_quality
COMMENT 'Lifetime complaints and units shipped per manufacturing lot, with supplier. Synthetic.'
AS
WITH shipped AS (SELECT lot_id, sum(units_shipped) AS units_shipped FROM shipments GROUP BY ALL),
complained AS (
  SELECT lot_id, count(*) AS complaints, count_if(patient_injury) AS injury_complaints,
         count_if(service_failure_confirmed) AS confirmed_failures
  FROM complaints_enriched WHERE lot_id IS NOT NULL GROUP BY ALL
)
SELECT l.lot_id, l.family_id, f.family_name, l.build_date, date_trunc('MONTH', l.build_date) AS build_month, l.site,
       s.supplier_name AS key_component_supplier, f.key_component,
       coalesce(sh.units_shipped, 0)     AS units_shipped,
       coalesce(c.complaints, 0)         AS complaints,
       coalesce(c.injury_complaints, 0)  AS injury_complaints,
       coalesce(c.confirmed_failures, 0) AS confirmed_failures
FROM manufacturing_lots l
JOIN device_families f ON l.family_id = f.family_id
JOIN suppliers s ON l.supplier_id = s.supplier_id
LEFT JOIN shipped sh ON l.lot_id = sh.lot_id
LEFT JOIN complained c ON l.lot_id = c.lot_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric views: one definition of complaint rate

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW pms_monthly_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: {catalog}.{schema}.monthly_quality
comment: "Post-market complaint metrics by device family, month and region. Complaint rate = complaints received in the period per 1,000 units shipped in the same period."
dimensions:
  - name: Device Family
    expr: family_name
    synonyms: [product, device, product family]
  - name: Month
    expr: month
  - name: Quarter
    expr: "DATE_TRUNC('QUARTER', month)"
  - name: Region
    expr: region
measures:
  - name: Units Shipped
    expr: SUM(units_shipped)
  - name: Complaints
    expr: SUM(complaints)
    synonyms: [complaint count, number of complaints]
  - name: Injury Complaints
    expr: SUM(injury_complaints)
    synonyms: [complaints with patient injury, harm complaints]
  - name: Complaint Rate per 1000 Units
    expr: SUM(complaints) * 1000.0 / NULLIF(SUM(units_shipped), 0)
    synonyms: [complaint rate, complaints per thousand]
  - name: Cuff Deflation Complaints
    expr: SUM(cuff_deflation_complaints)
$$
""")

spark.sql(f"""
CREATE OR REPLACE VIEW pms_lot_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: {catalog}.{schema}.lot_quality
comment: "Lifetime complaint metrics per manufacturing lot and key-component supplier. Complaint rate = complaints per 1,000 units shipped from the lot."
dimensions:
  - name: Lot
    expr: lot_id
    synonyms: [lot number, batch]
  - name: Device Family
    expr: family_name
  - name: Build Month
    expr: build_month
  - name: Manufacturing Site
    expr: site
  - name: Key Component Supplier
    expr: key_component_supplier
    synonyms: [supplier, vendor]
  - name: Key Component
    expr: key_component
measures:
  - name: Units Shipped
    expr: SUM(units_shipped)
  - name: Complaints
    expr: SUM(complaints)
  - name: Injury Complaints
    expr: SUM(injury_complaints)
  - name: Confirmed Failures
    expr: SUM(confirmed_failures)
    synonyms: [service confirmed failures]
  - name: Complaint Rate per 1000 Units
    expr: SUM(complaints) * 1000.0 / NULLIF(SUM(units_shipped), 0)
    synonyms: [complaint rate, lot complaint rate]
$$
""")

display(spark.sql("""
SELECT `Device Family`, `Quarter`, MEASURE(`Complaints`) AS complaints,
       round(MEASURE(`Complaint Rate per 1000 Units`), 2) AS complaint_rate_per_1k
FROM pms_monthly_metrics GROUP BY ALL ORDER BY 1, 2
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance
# MAGIC
# MAGIC * **Column comments** give Genie and people the business meaning of each field.
# MAGIC * **Tags** classify the tables for discovery and policy.
# MAGIC * **Column mask**: reporter contact details are visible only to members of the `pms_privacy_officers` group.
# MAGIC   Genie runs queries as the person asking, so the mask applies to Genie answers automatically.

# COMMAND ----------

column_comments = {
    "complaints_enriched": {
        "failure_mode": "Failure mode extracted from the complaint narrative by ai_extract.",
        "patient_injury": "True when the narrative describes harm to a person. Extracted by ai_extract.",
        "lot_id": "Manufacturing lot. From the intake record when present, otherwise extracted from the narrative.",
        "lot_source": "Where lot_id came from: intake record, extracted from narrative, or unknown.",
        "key_component_supplier": "Supplier of the key component for the lot.",
        "service_failure_confirmed": "True when service evaluation of the returned unit confirmed a defect.",
        "reporter_contact": "Reporter contact details. Masked for users outside pms_privacy_officers.",
        "narrative": "Original free-text complaint narrative as received.",
    },
    "capa_documents": {
        "affected_lots": "Lots named in the CAPA report.",
        "document_text": "Full text of the CAPA PDF parsed by ai_parse_document.",
        "source_file": "Path of the source PDF in the documents volume.",
    },
}
for table, cols in column_comments.items():
    for col, text in cols.items():
        escaped = text.replace("'", "\\'")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{escaped}'")

spark.sql("""
CREATE OR REPLACE FUNCTION mask_reporter_contact(contact STRING)
RETURN CASE WHEN is_account_group_member('pms_privacy_officers') THEN contact ELSE '[redacted]' END
""")
spark.sql("ALTER TABLE complaints_enriched ALTER COLUMN reporter_contact SET MASK mask_reporter_contact")

for table in ["complaints_enriched", "monthly_quality", "lot_quality", "capa_documents", "service_findings"]:
    try:
        spark.sql(f"ALTER TABLE {table} SET TAGS ('domain' = 'post_market_surveillance', 'data' = 'synthetic')")
    except Exception as e:  # tag policies in some workspaces restrict allowed keys or values
        print(f"Skipped tags on {table}: {e}")

display(spark.sql("SELECT complaint_id, reporter_contact, failure_mode FROM complaints_enriched LIMIT 5"))
