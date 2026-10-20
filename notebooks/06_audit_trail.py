# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Audit trail: who asked what, and which SQL answered it
# MAGIC
# MAGIC In a regulated quality system, an answer is only useful if it can be traced. Every Genie interaction leaves two
# MAGIC records in Unity Catalog system tables:
# MAGIC
# MAGIC | System table | What it records |
# MAGIC |---|---|
# MAGIC | `system.access.audit` | Each Genie action (conversation started, message sent, result viewed) with user, time, and space ID |
# MAGIC | `system.query.history` | Every SQL statement Genie executed, with the user it ran as, full SQL text, and duration |
# MAGIC
# MAGIC Run this notebook after asking the agent a few questions. System tables update with a short delay, and reading
# MAGIC them requires `SELECT` on the `system.access` and `system.query` schemas.

# COMMAND ----------

dbutils.widgets.text("genie_space_id", "")
space_id = dbutils.widgets.get("genie_space_id")
assert space_id, "Set the genie_space_id widget to the ID printed by notebook 05."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie actions in the audit log

# COMMAND ----------

display(spark.sql("""
SELECT event_time,
       user_identity.email                AS user,
       action_name,
       request_params['conversation_id']  AS conversation_id,
       request_params['message_id']       AS message_id,
       response.status_code               AS status
FROM system.access.audit
WHERE service_name = 'aibiGenie'
  AND request_params['space_id'] = :space_id
  AND event_date >= current_date() - 30
ORDER BY event_time DESC
LIMIT 200
""", args={"space_id": space_id}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL executed by Genie
# MAGIC
# MAGIC `executed_by` shows that each query ran with the asking user's identity, so table grants and the column
# MAGIC mask on reporter contact details were enforced for that user.

# COMMAND ----------

display(spark.sql("""
SELECT start_time,
       executed_by,
       execution_status,
       total_duration_ms,
       statement_text
FROM system.query.history
WHERE query_source.genie_space_id = :space_id
  AND start_time >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY start_time DESC
LIMIT 200
""", args={"space_id": space_id}))
