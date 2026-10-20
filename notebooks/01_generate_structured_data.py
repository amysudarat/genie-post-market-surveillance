# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate structured post-market data
# MAGIC
# MAGIC This notebook creates a fully synthetic dataset for a fictitious medical device maker with four device families.
# MAGIC No real company, product, patient, or complaint is represented.
# MAGIC
# MAGIC | Table | Grain | Description |
# MAGIC |---|---|---|
# MAGIC | `device_families` | one row per family | Product master (device class, launch date) |
# MAGIC | `suppliers` | one row per supplier | Component suppliers |
# MAGIC | `manufacturing_lots` | one row per lot | Build month, site, units built, key-component supplier |
# MAGIC | `shipments` | lot × ship month × region | Units shipped (denominator for complaint rates) |
# MAGIC | `complaints_raw` | one row per complaint | Intake record **plus free-text narrative** (unstructured) |
# MAGIC | `service_tickets_raw` | one row per returned unit | Repair record **plus free-text technician notes** (unstructured) |
# MAGIC
# MAGIC The data contains one planted quality signal: a set of blood pressure monitor lots built with valves from one supplier
# MAGIC fail more often. The lot number is missing from the structured complaint record roughly 45% of the time and appears only
# MAGIC in the narrative, which mirrors how complaint intake works in practice.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "pms_genie_cowork")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`documents`")
print(f"Target: {catalog}.{schema}")

# COMMAND ----------

import random
from datetime import date, timedelta

import numpy as np
import pandas as pd

rng = np.random.default_rng(42)
random.seed(42)

months = pd.period_range("2025-01", "2026-09", freq="M")
LAST_DAY = date(2026, 9, 30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data

# COMMAND ----------

families = pd.DataFrame(
    [
        ("BP3", "Blood Pressure Monitor", "Class II", "2023-04-01", "cuff valve"),
        ("OX5", "Pulse Oximeter", "Class II", "2022-09-01", "battery door"),
        ("TX2", "Digital Thermometer", "Class II", "2021-06-01", "probe sensor"),
        ("NB1", "Portable Nebulizer", "Class II", "2024-01-15", "compressor"),
    ],
    columns=["family_id", "family_name", "device_class", "launch_date", "key_component"],
)
families["launch_date"] = pd.to_datetime(families["launch_date"]).dt.date

suppliers = pd.DataFrame(
    [
        ("SUP-01", "Valve Supplier A", "cuff valve", "Germany"),
        ("SUP-02", "Valve Supplier B", "cuff valve", "Malaysia"),
        ("SUP-03", "Battery Door Supplier C", "battery door", "Sweden"),
        ("SUP-04", "Sensor Supplier D", "probe sensor", "USA"),
        ("SUP-05", "Compressor Supplier E", "compressor", "Mexico"),
        ("SUP-06", "Compressor Supplier F", "compressor", "USA"),
    ],
    columns=["supplier_id", "supplier_name", "component", "country"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manufacturing lots
# MAGIC
# MAGIC Two lots per family per month. Lots of the blood pressure monitor built in May and June 2026 receive cuff valves from Valve Supplier B
# MAGIC (`SUP-02`); all other blood pressure monitor lots use Valve Supplier A (`SUP-01`).

# COMMAND ----------

BAD_LOTS = {"BP3-2605-A", "BP3-2605-B", "BP3-2606-A"}
component_supplier = {"BP3": "SUP-01", "OX5": "SUP-03", "TX2": "SUP-04"}

lots = []
for fam in families.itertuples():
    for m in months:
        for suffix in ("A", "B"):
            lot_id = f"{fam.family_id}-{m.strftime('%y%m')}-{suffix}"
            if fam.family_id == "BP3":
                supplier = "SUP-02" if lot_id in BAD_LOTS else "SUP-01"
            elif fam.family_id == "NB1":
                supplier = "SUP-05" if suffix == "A" else "SUP-06"
            else:
                supplier = component_supplier[fam.family_id]
            lots.append(
                (
                    lot_id,
                    fam.family_id,
                    supplier,
                    m.start_time.date() + timedelta(days=int(rng.integers(0, 20))),
                    "Plant 1 - Ohio" if suffix == "A" else "Plant 2 - Ireland",
                    int(rng.integers(7000, 9500)),
                )
            )
lots = pd.DataFrame(lots, columns=["lot_id", "family_id", "supplier_id", "build_date", "site", "units_built"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shipments
# MAGIC
# MAGIC Each lot ships over three months (45% / 35% / 20%) across three regions.

# COMMAND ----------

REGIONS = {"North America": 0.5, "Europe": 0.3, "Asia Pacific": 0.2}
ship_rows = []
for lot in lots.itertuples():
    build_month = pd.Period(lot.build_date, freq="M")
    for offset, share in enumerate((0.45, 0.35, 0.20)):
        ship_month = build_month + offset
        if ship_month > months[-1]:
            continue
        for region, rshare in REGIONS.items():
            ship_rows.append(
                (lot.lot_id, lot.family_id, ship_month.start_time.date(), region, int(lot.units_built * share * rshare))
            )
shipments = pd.DataFrame(ship_rows, columns=["lot_id", "family_id", "ship_month", "region", "units_shipped"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complaint narratives
# MAGIC
# MAGIC Narratives are assembled from phrase templates so that wording varies the way free-text intake does.
# MAGIC Failure mode, injury, and lot number exist only in the text; the structured record holds intake metadata.

# COMMAND ----------

FAILURE_TEXT = {
    "BP3": {
        "cuff_deflation": [
            "the cuff inflated but would not deflate after the reading",
            "cuff stayed tight on the arm and did not release air",
            "device kept the cuff pressurized for several minutes; user had to pull it off",
            "slow deflation, cuff remained inflated well after the measurement finished",
        ],
        "inaccurate_reading": [
            "systolic reading about 25 points higher than the clinic monitor",
            "readings jump between measurements taken one minute apart",
        ],
        "display_error": ["screen shows error E4 and goes blank", "display segments missing, numbers unreadable"],
        "battery": ["batteries drain within two days", "unit turns off mid-measurement"],
    },
    "OX5": {
        "battery": ["battery door does not latch and batteries fall out", "device powers off when tapped"],
        "inaccurate_reading": ["SpO2 shows 85% on a healthy adult", "pulse rate frozen at 0"],
        "display_error": ["display flickers and dims"],
    },
    "TX2": {
        "inaccurate_reading": ["reads 2 degrees low compared to another thermometer", "temperature varies on repeat readings"],
        "physical_damage": ["probe tip cover cracked out of the box"],
        "battery": ["low battery icon on a new unit"],
    },
    "NB1": {
        "compressor": ["compressor very loud and output weak", "motor overheats after ten minutes of use"],
        "physical_damage": ["tubing connector broke off"],
        "connectivity": ["companion app cannot pair with the unit"],
    },
}
INJURY_TEXT = {
    "cuff_deflation": [
        "Patient reported bruising on the upper arm.",
        "Patient described arm pain and numbness that lasted about 20 minutes.",
        "Caregiver noted redness and swelling at the cuff site.",
    ],
    "default": ["Patient felt dizzy and sought a second opinion from their doctor."],
}
OPENERS = [
    "Customer called to report that {f}.",
    "Caller states {f}.",
    "Per email from the user: {f}.",
    "Field rep relayed a report that {f}.",
    "Home user reports {f}.",
]
LOT_PHRASES = ["Lot {lot} on the box.", "Unit is from lot {lot}.", "Label shows lot no. {lot}.", "LOT: {lot}."]
CLOSERS = [
    "No injury reported.",
    "Customer requested a replacement.",
    "Unit will be returned for evaluation.",
    "User has stopped using the device.",
    "",
]
CHANNELS = ["Call center", "Email", "Field representative", "Web portal"]
COUNTRIES = {
    "North America": ["United States", "Canada"],
    "Europe": ["Germany", "France", "Ireland", "Spain"],
    "Asia Pacific": ["Japan", "Australia", "Singapore"],
}

# COMMAND ----------

BASE_RATE_PER_1K = {"BP3": 0.55, "OX5": 0.60, "TX2": 0.45, "NB1": 0.70}
DEFECT_RATE_PER_1K = 5.5  # extra cuff-deflation complaints per 1,000 units on affected lots

complaints, tickets = [], []
cid, tid = 100000, 500000

for s in shipments.itertuples():
    fam = s.family_id
    n_base = rng.poisson(s.units_shipped / 1000 * BASE_RATE_PER_1K[fam])
    n_defect = rng.poisson(s.units_shipped / 1000 * DEFECT_RATE_PER_1K) if s.lot_id in BAD_LOTS else 0
    events = [None] * n_base + ["cuff_deflation"] * n_defect
    for forced_mode in events:
        if forced_mode:
            mode, lag = forced_mode, int(rng.integers(20, 75))
        else:
            modes = [m for m in FAILURE_TEXT[fam] if not (fam == "BP3" and m == "cuff_deflation")] + (
                ["cuff_deflation"] if fam == "BP3" and rng.random() < 0.15 else []
            )
            mode, lag = random.choice(modes), int(rng.integers(5, 90))
        received = s.ship_month + timedelta(days=lag)
        if received > LAST_DAY:
            continue
        injury = rng.random() < (0.22 if mode == "cuff_deflation" else 0.03)
        lot_known = rng.random() < 0.85
        lot_in_record = lot_known and rng.random() < 0.55

        parts = [random.choice(OPENERS).format(f=random.choice(FAILURE_TEXT[fam][mode]))]
        if lot_known:
            parts.append(random.choice(LOT_PHRASES).format(lot=s.lot_id))
        parts.append(random.choice(INJURY_TEXT.get(mode, INJURY_TEXT["default"])) if injury else random.choice(CLOSERS))
        cid += 1
        complaints.append(
            (
                f"CMP-{cid}",
                received,
                fam,
                s.lot_id if lot_in_record else None,
                random.choice(CHANNELS),
                random.choice(COUNTRIES[s.region]),
                s.region,
                f"reporter{cid}@example.com",
                " ".join(p for p in parts if p),
            )
        )

        if rng.random() < 0.35:  # unit returned and evaluated by service
            tid += 1
            if mode == "cuff_deflation" and s.lot_id in BAD_LOTS:
                note = random.choice(
                    [
                        "Reproduced failure. Valve seal deformed and sticking closed. Replaced cuff valve assembly VA-1.",
                        "Valve does not vent below 40 mmHg. Seal material appears soft. Replaced valve VA-1, retested OK.",
                        "Cuff release valve sticking. Swapped VA-1 valve, unit passes deflation test.",
                    ]
                )
                code = "REPLACED_PART"
            elif rng.random() < 0.45:
                note, code = "Could not reproduce reported issue. Unit passes all functional tests.", "NO_FAULT_FOUND"
            else:
                note = random.choice(
                    [
                        "Battery contacts corroded, cleaned and replaced battery spring.",
                        "Display ribbon cable loose, reseated and tested.",
                        "Sensor recalibrated to factory spec.",
                        "Housing cracked near connector, replaced housing.",
                    ]
                )
                code = "REPAIRED"
            tickets.append(
                (
                    f"SVC-{tid}",
                    f"CMP-{cid}",
                    min(received + timedelta(days=int(rng.integers(7, 21))), LAST_DAY),
                    fam,
                    s.lot_id,
                    note,
                    code,
                )
            )

complaints = pd.DataFrame(
    complaints,
    columns=["complaint_id", "received_date", "family_id", "lot_id", "intake_channel", "country", "region",
             "reporter_contact", "narrative"],
)
tickets = pd.DataFrame(
    tickets, columns=["ticket_id", "complaint_id", "evaluated_date", "family_id", "lot_id", "technician_notes", "resolution_code"]
)
print(f"lots={len(lots)} shipments={len(shipments)} complaints={len(complaints)} service_tickets={len(tickets)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta tables

# COMMAND ----------

def write(pdf: pd.DataFrame, name: str, comment: str) -> None:
    full = f"`{catalog}`.`{schema}`.`{name}`"
    spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    escaped = comment.replace("'", "\\'")
    spark.sql(f"COMMENT ON TABLE {full} IS '{escaped}'")


write(families, "device_families", "Device family master data. Synthetic.")
write(suppliers, "suppliers", "Key-component suppliers. Synthetic.")
write(lots, "manufacturing_lots", "One row per manufacturing lot with the supplier of the key component. Synthetic.")
write(shipments, "shipments", "Units shipped per lot, month and region. Denominator for complaint rates. Synthetic.")
write(complaints, "complaints_raw", "Complaint intake records with free-text narrative. lot_id is often blank. Synthetic.")
write(tickets, "service_tickets_raw", "Returned-unit evaluations with free-text technician notes. Synthetic.")

display(spark.sql(f"SELECT family_id, date_trunc('quarter', received_date) AS quarter, count(*) AS complaints "
                  f"FROM `{catalog}`.`{schema}`.complaints_raw GROUP BY ALL ORDER BY 1, 2"))
