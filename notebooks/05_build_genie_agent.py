# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Build the Genie agent
# MAGIC
# MAGIC A Genie agent is a curated, governed AI coworker scoped to one business domain. Its quality depends on
# MAGIC four things, all defined in this notebook as code so they can be reviewed and versioned:
# MAGIC
# MAGIC | Component | Purpose in this agent |
# MAGIC |---|---|
# MAGIC | **Data sources** | Two metric views (certified complaint rate) and three tables (complaints, service findings, CAPA documents) |
# MAGIC | **Instructions** | Domain rules: compare rates not counts, check CAPAs and service findings, never decide reportability |
# MAGIC | **Example SQL** | Trusted queries for the questions the quality team asks most |
# MAGIC | **Benchmarks** | Questions with known answers used to evaluate the agent before rollout |
# MAGIC
# MAGIC The agent queries data with the permissions of the person asking. Unity Catalog grants and the column mask on
# MAGIC reporter contact details apply to every answer.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "pms_genie_cowork")
dbutils.widgets.text("warehouse_id", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")

import json
import uuid

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
if not warehouse_id:
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise ValueError("No SQL warehouse found. Create one or set the warehouse_id widget.")
    warehouse_id = warehouses[0].id
me = w.current_user.me().user_name
parent_path = f"/Workspace/Users/{me}/genie_agents"
w.workspace.mkdirs(parent_path)
fq = lambda name: f"{catalog}.{schema}.{name}"  # noqa: E731
print(f"warehouse={warehouse_id} parent={parent_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instructions

# COMMAND ----------

INSTRUCTIONS = [
    "You support a medical device post-market surveillance and quality team.",
    "Complaint rate is defined in the metric views as complaints per 1,000 units shipped. Use the measure "
    "`Complaint Rate per 1000 Units`; do not compute your own rate from raw counts.",
    "When asked why complaints changed, compare complaint rates (not counts) across device family, lot, "
    "key component supplier, manufacturing site and failure mode. Use pms_lot_metrics for lot and supplier comparisons.",
    "After finding a driver, check capa_documents for an investigation that names the same lots or component, and "
    "service_findings for confirmed failures on returned units.",
    "failure_mode and patient_injury were extracted from free-text narratives by AI. When counts matter for a decision, "
    "say so and offer example narratives from complaints_enriched so a person can verify.",
    "patient_injury = true marks a complaint for regulatory reportability assessment. Never state that a complaint is "
    "or is not reportable; that decision belongs to the regulatory team.",
    "lot_source shows whether the lot came from the intake record or was extracted from the narrative.",
    "Quarters are calendar quarters. 'Last quarter' means the most recent complete quarter in the data.",
    "Always name the tables or metric views used in the answer.",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trusted example SQL

# COMMAND ----------

EXAMPLES = [
    (
        "What is the complaint rate by device family by quarter?",
        f"""SELECT `Device Family`, `Quarter`, MEASURE(`Complaints`) AS complaints,
       ROUND(MEASURE(`Complaint Rate per 1000 Units`), 2) AS complaint_rate_per_1k
FROM {fq('pms_monthly_metrics')}
GROUP BY ALL ORDER BY `Device Family`, `Quarter`""",
    ),
    (
        "Which lots have the highest complaint rate?",
        f"""SELECT `Lot`, `Device Family`, `Key Component Supplier`, `Build Month`, MEASURE(`Units Shipped`) AS units_shipped,
       MEASURE(`Complaints`) AS complaints, ROUND(MEASURE(`Complaint Rate per 1000 Units`), 2) AS complaint_rate_per_1k
FROM {fq('pms_lot_metrics')}
GROUP BY ALL ORDER BY complaint_rate_per_1k DESC LIMIT 10""",
    ),
    (
        "Compare complaint rate by supplier for the blood pressure monitor",
        f"""SELECT `Key Component Supplier`, MEASURE(`Units Shipped`) AS units_shipped, MEASURE(`Complaints`) AS complaints,
       ROUND(MEASURE(`Complaint Rate per 1000 Units`), 2) AS complaint_rate_per_1k
FROM {fq('pms_lot_metrics')}
WHERE `Device Family` = 'Blood Pressure Monitor'
GROUP BY ALL ORDER BY complaint_rate_per_1k DESC""",
    ),
    (
        "Show failure modes for a device family by month",
        f"""SELECT complaints_enriched.received_month, complaints_enriched.failure_mode, COUNT(*) AS complaints,
       COUNT_IF(complaints_enriched.patient_injury) AS injury_complaints
FROM {fq('complaints_enriched')} AS complaints_enriched
WHERE complaints_enriched.family_name = 'Blood Pressure Monitor'
GROUP BY ALL ORDER BY complaints_enriched.received_month, complaints DESC""",
    ),
    (
        "Is there a CAPA that covers lot BP3-2605-A?",
        f"""SELECT capa_documents.capa_number, capa_documents.status, capa_documents.date_opened, capa_documents.component,
       capa_documents.supplier, capa_documents.root_cause, capa_documents.risk_rating
FROM {fq('capa_documents')} AS capa_documents
WHERE array_contains(capa_documents.affected_lots, 'BP3-2605-A')""",
    ),
    (
        "How many complaints only had the lot number in the narrative?",
        f"""SELECT complaints_enriched.lot_source, COUNT(*) AS complaints
FROM {fq('complaints_enriched')} AS complaints_enriched
GROUP BY ALL ORDER BY complaints DESC""",
    ),
]

SAMPLE_QUESTIONS = [
    "Complaints have been going up lately. What's driving it, and is anyone already looking into it?",
    "Why did blood pressure monitor complaints rise in Q3 2026?",
    "Which lots have the highest complaint rate?",
    "Is there an open CAPA for the blood pressure monitor cuff deflation issue?",
    "How many complaints with a patient injury were received last quarter, by device family?",
    "What did service find on returned blood pressure monitor units from lot BP3-2605-B?",
]

BENCHMARKS = [
    (
        "Which key component supplier has the highest complaint rate for the blood pressure monitor?",
        f"""SELECT `Key Component Supplier`, ROUND(MEASURE(`Complaint Rate per 1000 Units`), 2) AS complaint_rate_per_1k
FROM {fq('pms_lot_metrics')} WHERE `Device Family` = 'Blood Pressure Monitor'
GROUP BY ALL ORDER BY complaint_rate_per_1k DESC LIMIT 1""",
        None,
    ),
    (
        "How many complaints were received in Q3 2026 by device family?",
        f"""SELECT `Device Family`, MEASURE(`Complaints`) AS complaints FROM {fq('pms_monthly_metrics')}
WHERE `Quarter` = DATE'2026-07-01' GROUP BY ALL ORDER BY complaints DESC""",
        None,
    ),
    (
        "Complaints have been going up lately. What's driving it, and is anyone already looking into it?",
        None,
        "Must identify the blood pressure monitor as the driver and cuff deflation failures concentrated in lots BP3-2605-A, BP3-2605-B and BP3-2606-A with valves "
        "from Valve Supplier B, using complaint rate rather than counts. Should cite CAPA-2026-014 and confirmed "
        "valve failures from service findings. Must not state a reportability decision.",
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data sources and column configuration

# COMMAND ----------

def col(name, description=None, synonyms=None, match=False, exclude=False):
    c = {"column_name": name}
    if description:
        c["description"] = [description]
    if synonyms:
        c["synonyms"] = synonyms
    if match:
        c["enable_format_assistance"] = True
        c["enable_entity_matching"] = True
    if exclude:
        c["exclude"] = True
    return c


tables = [
    {
        "identifier": fq("capa_documents"),
        "column_configs": [col("capa_number", synonyms=["CAPA", "CAPA id"]), col("status", match=True)],
    },
    {
        "identifier": fq("complaints_enriched"),
        "column_configs": [
            col("failure_mode", synonyms=["issue", "problem type", "defect"], match=True),
            col("family_name", synonyms=["device", "product"], match=True),
            col("key_component_supplier", synonyms=["supplier", "vendor"], match=True),
            col("lot_source", match=True),
            col("region", match=True),
        ],
    },
    {"identifier": fq("service_findings"), "column_configs": [col("resolution_code", match=True)]},
]
metric_views = [
    {"identifier": fq("pms_lot_metrics"), "column_configs": [col("Device Family", match=True), col("Key Component Supplier", match=True)]},
    {"identifier": fq("pms_monthly_metrics"), "column_configs": [col("Device Family", match=True), col("Region", match=True)]},
]
for ds in tables + metric_views:
    ds["column_configs"].sort(key=lambda c: c["column_name"])

newid = lambda: uuid.uuid4().hex  # noqa: E731

serialized = {
    "version": 2,
    "config": {"sample_questions": sorted([{"id": newid(), "question": [q]} for q in SAMPLE_QUESTIONS], key=lambda x: x["id"])},
    "data_sources": {
        "tables": sorted(tables, key=lambda t: t["identifier"]),
        "metric_views": sorted(metric_views, key=lambda t: t["identifier"]),
    },
    "instructions": {
        "text_instructions": [{"id": newid(), "content": [line + "\n" for line in INSTRUCTIONS]}],
        "example_question_sqls": sorted(
            [{"id": newid(), "question": [q], "sql": [s]} for q, s in EXAMPLES], key=lambda x: x["id"]
        ),
    },
    "benchmarks": {
        "questions": sorted(
            [
                {
                    "id": newid(),
                    "question": [q],
                    "answer": [{"format": "SQL", "content": [s] if s else []}],
                    **({"evaluation_note": [note]} if note else {}),
                }
                for q, s, note in BENCHMARKS
            ],
            key=lambda x: x["id"],
        )
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update the agent

# COMMAND ----------

TITLE = "Post-Market Surveillance Coworker"
DESCRIPTION = (
    "Ask about complaint trends, lots, suppliers, service findings and CAPA investigations for the device portfolio. "
    "Synthetic demo data."
)

existing, page_token = [], None
while True:
    page = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_token": page_token} if page_token else None)
    existing += page.get("spaces", [])
    page_token = page.get("next_page_token")
    if not page_token:
        break
match = next((s for s in existing if s.get("title") == TITLE), None)
body = {"warehouse_id": warehouse_id, "title": TITLE, "description": DESCRIPTION,
        "serialized_space": json.dumps(serialized)}

if match:
    space_id = match["space_id"]
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
    print(f"Updated Genie agent {space_id}")
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body={**body, "parent_path": parent_path})["space_id"]
    print(f"Created Genie agent {space_id}")

host = w.config.host.rstrip("/")
print(f"Open the agent: {host}/genie/rooms/{space_id}")
