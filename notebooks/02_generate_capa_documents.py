# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Generate unstructured CAPA documents
# MAGIC
# MAGIC Quality teams record investigations as documents, not tables. This notebook renders four synthetic
# MAGIC CAPA (Corrective and Preventive Action) reports as PDFs and writes them to the Unity Catalog volume
# MAGIC `documents/capa/`. Notebook 03 parses them with `ai_parse_document` and extracts structured fields.

# COMMAND ----------

import os

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "pms_genie_cowork")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
out_dir = f"/Volumes/{catalog}/{schema}/documents/capa"
os.makedirs(out_dir, exist_ok=True)

# COMMAND ----------

CAPAS = [
    {
        "id": "CAPA-2025-031",
        "title": "Thermometer probe tip cover cracking at receipt",
        "opened": "2025-07-14",
        "status": "Closed - effective",
        "family": "Digital Thermometer",
        "component": "probe tip cover",
        "supplier": "Sensor Supplier D",
        "lots": "TX2-2505-A, TX2-2505-B",
        "problem": "Complaint trending flagged cracked probe tip covers on units opened by end users. "
        "Six complaints over eight weeks, no patient injury.",
        "investigation": "Drop testing of packaged units reproduced cracking when the inner tray was omitted. "
        "Packaging line records show the tray feeder jammed for two shifts in May 2025.",
        "root_cause": "Packaging nonconformance: inner tray omitted during a feeder jam.",
        "actions": "Added tray-presence vision check to the packaging line. Retrained operators. "
        "Effectiveness check after 90 days showed zero recurrence.",
        "risk": "Low",
    },
    {
        "id": "CAPA-2026-007",
        "title": "Pulse oximeter battery door latch does not engage",
        "opened": "2026-02-02",
        "status": "Closed - effective",
        "family": "Pulse Oximeter",
        "component": "battery door",
        "supplier": "Battery Door Supplier C",
        "lots": "OX5-2511-B",
        "problem": "Battery door complaints rose in December 2025, concentrated in one lot.",
        "investigation": "Dimensional inspection found the latch tab 0.2 mm under minimum on one mold cavity.",
        "root_cause": "Worn mold cavity at the supplier produced undersized latch tabs.",
        "actions": "Supplier replaced the cavity insert and added first-article inspection per shift. "
        "Lot OX5-2511-B placed on hold for remaining inventory.",
        "risk": "Low",
    },
    {
        "id": "CAPA-2026-014",
        "title": "Blood pressure monitor cuff fails to deflate after measurement",
        "opened": "2026-09-08",
        "status": "Open - investigation",
        "family": "Blood Pressure Monitor",
        "component": "cuff valve",
        "supplier": "Valve Supplier B",
        "lots": "BP3-2605-A, BP3-2605-B, BP3-2606-A",
        "problem": "Complaints describing a cuff that stays inflated after the reading increased sharply from June 2026. "
        "Several reports describe bruising, arm pain, or numbness. Returned units reproduce slow or no deflation.",
        "investigation": "Service evaluations show the release valve seal deforming and sticking closed. All confirmed "
        "failures trace to valves from Valve Supplier B, a second-source supplier qualified in April 2026. "
        "Durometer testing of retained valve samples shows seal hardness below specification.",
        "root_cause": "Preliminary: valve seal material from the second-source supplier is softer than specified, "
        "causing the seal to stick under repeated inflation cycles.",
        "actions": "Containment: quarantine remaining Valve Supplier B inventory and affected finished goods. "
        "Evaluate field action for the three affected lots. Assess regulatory reportability of injury complaints.",
        "risk": "High",
    },
    {
        "id": "CAPA-2026-016",
        "title": "Nebulizer companion app pairing failures",
        "opened": "2026-08-19",
        "status": "Closed - no product change",
        "family": "Portable Nebulizer",
        "component": "companion app",
        "supplier": "Internal software",
        "lots": "Not lot specific",
        "problem": "Users report the companion app cannot pair with the nebulizer.",
        "investigation": "Failures occur only on one phone operating system version. Device firmware is unaffected.",
        "root_cause": "Third-party phone OS update changed Bluetooth permission prompts.",
        "actions": "App update released with revised permission flow. No change to the medical device.",
        "risk": "Low",
    },
]

# COMMAND ----------

import textwrap


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def render(capa: dict, path: str) -> None:
    """Write a single-page text PDF using only the Python standard library."""
    lines = [
        ("F2", 16, "Corrective and Preventive Action Report"),
        ("F2", 12, capa["title"]),
        ("F1", 10, ""),
        ("F1", 10, f"CAPA number: {capa['id']}        Status: {capa['status']}"),
        ("F1", 10, f"Date opened: {capa['opened']}        Risk rating: {capa['risk']}"),
        ("F1", 10, f"Device family: {capa['family']}"),
        ("F1", 10, f"Component: {capa['component']}        Supplier: {capa['supplier']}"),
        ("F1", 10, f"Affected lots: {capa['lots']}"),
    ]
    for heading, key in [
        ("1. Problem description", "problem"),
        ("2. Investigation", "investigation"),
        ("3. Root cause", "root_cause"),
        ("4. Corrective and preventive actions", "actions"),
    ]:
        lines += [("F1", 10, ""), ("F2", 11, heading)]
        lines += [("F1", 10, row) for row in textwrap.wrap(capa[key], 95)]
    lines += [("F1", 10, ""), ("F1", 8, "Synthetic document generated for demonstration purposes.")]

    ops, y = [], 750
    for font, size, text in lines:
        ops.append(f"BT /{font} {size} Tf 50 {y} Td ({_pdf_escape(text)}) Tj ET")
        y -= size + 6
    stream = "\n".join(ops).encode("latin-1")

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 4 0 R /F2 5 0 R >> >> /Contents 6 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
    ]
    out, offsets = bytearray(b"%PDF-1.4\n"), []
    for i, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    out += b"".join(f"{o:010d} 00000 n \n".encode() for o in offsets)
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    with open(path, "wb") as f:
        f.write(out)


for capa in CAPAS:
    render(capa, f"{out_dir}/{capa['id']}.pdf")  # volumes are writable as regular file paths

display(dbutils.fs.ls(out_dir))
