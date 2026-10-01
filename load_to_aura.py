#!/usr/bin/env python3
"""
load_to_aura.py - load the simple genealogy dataset into Neo4j AuraDB Free (or any Neo4j 5+).

Aura can't read files from your laptop, so this script reads the CSVs locally and sends
them in batches through the official Python driver.

    pip install -r requirements.txt
    python load_to_aura.py            # load (safe to re-run: uses MERGE, no duplicates)
    python load_to_aura.py --wipe     # delete EVERYTHING in the database first

Credentials come from aura.env (the file Aura lets you download when you create the
instance) or from NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD environment variables.

Graph model
  (:Lot:RawMaterial | :Lot:Intermediate | :Lot:Cell | :Lot:Package {lot_id, lot_type, ...})
  (:Lot)-[:USED_IN {step, ts, qty, uom, pieces}]->(:Lot)          genealogy (parent -> child)
  (:Lot)-[:PROCESSED_ON {step, ts, recipe_id, operator_id, events}]->(:Equipment)
  (:Lot)-[:STORED_AT {ts, dew_point_c}]->(:Location)
  (:Cell)-[:IN_COHORT]->(:Cohort {name, scenario, role, members})  ready-made analysis groups
"""
import argparse
import csv
import glob
import json
import os
import sys
import time

from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ConfigurationError, ServiceUnavailable

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
BATCH = 2000
KINDS = ("RawMaterial", "Intermediate", "Cell", "Package")   

def load_env(path):
    """Read KEY=VALUE lines (the Aura credentials file format) into os.environ."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def read(name):
    """CSV rows as dicts; empty strings become None so Neo4j doesn't store empty properties."""
    with open(os.path.join(DATA, name), newline="", encoding="utf-8") as f:
        return [{k: (v if v != "" else None) for k, v in r.items()} for r in csv.DictReader(f)]


def num(x):
    return None if x is None else float(x)


def chunks(rows, n=BATCH):
    for i in range(0, len(rows), n):
        yield rows[i:i + n]


SCHEMA = [
    "CREATE CONSTRAINT lot_id IF NOT EXISTS FOR (l:Lot) REQUIRE l.lot_id IS UNIQUE",
    "CREATE CONSTRAINT equipment_id IF NOT EXISTS FOR (e:Equipment) REQUIRE e.equipment_id IS UNIQUE",
    "CREATE CONSTRAINT location_id IF NOT EXISTS FOR (l:Location) REQUIRE l.location_id IS UNIQUE",
    "CREATE CONSTRAINT cohort_name IF NOT EXISTS FOR (k:Cohort) REQUIRE k.name IS UNIQUE",
    "CREATE INDEX lot_batch IF NOT EXISTS FOR (l:Lot) ON (l.batch)",
    "CREATE INDEX lot_type IF NOT EXISTS FOR (l:Lot) ON (l.lot_type)",
]

Q_LOTS = """
UNWIND $rows AS r
MERGE (l:Lot {lot_id: r.lot_id})
SET l:%s,
    l.lot_type = r.lot_type, l.product = r.product, l.batch = r.batch,
    l.qty = r.qty, l.uom = r.uom, l.created_ts = localdatetime(r.created_ts),
    l.supplier = r.supplier, l.status = r.status, l.eol_result = r.eol_result,
    l.fail_mode = r.fail_mode, l.eol_ts = localdatetime(r.eol_ts)
"""
Q_EQUIPMENT = """
UNWIND $rows AS r
MERGE (e:Equipment {equipment_id: r.equipment_id})
SET e.equipment_type = r.equipment_type
"""
Q_LOCATIONS = """
UNWIND $rows AS r
MERGE (l:Location {location_id: r.location_id})
SET l.area = r.area
"""
Q_USED_IN = """
UNWIND $rows AS r
MATCH (p:Lot {lot_id: r.parent_lot_id})
MATCH (c:Lot {lot_id: r.child_lot_id})
MERGE (p)-[u:USED_IN]->(c)
SET u.step = r.step, u.ts = localdatetime(r.ts), u.qty = r.qty, u.uom = r.uom, u.pieces = r.pieces
"""
Q_PROCESSED_ON = """
UNWIND $rows AS r
MATCH (l:Lot {lot_id: r.lot_id})
MATCH (e:Equipment {equipment_id: r.equipment_id})
MERGE (l)-[p:PROCESSED_ON {step: r.step}]->(e)
SET p.ts = localdatetime(r.ts), p.recipe_id = r.recipe_id, p.operator_id = r.operator_id,
    p.events = r.events
"""
Q_STORED_AT = """
UNWIND $rows AS r
MATCH (l:Lot {lot_id: r.lot_id})
MATCH (loc:Location {location_id: r.location_id})
MERGE (l)-[s:STORED_AT {ts: localdatetime(r.ts)}]->(loc)
SET s.dew_point_c = r.dew_point_c
"""
Q_COHORT = """
MERGE (k:Cohort {name: $name})
SET k.scenario = $scenario, k.role = $role, k.members = size($ids)
WITH k
UNWIND $ids AS id
MATCH (c:Cell {lot_id: id})
MERGE (c)-[:IN_COHORT]->(k)
"""
Q_COUNTS = "RETURN COUNT { MATCH (n) } AS nodes, COUNT { MATCH ()-[r]->() } AS relationships"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--wipe", action="store_true", help="delete ALL nodes and relationships first")
    ap.add_argument("--env", default=os.path.join(HERE, "aura.env"), help="credentials file")
    args = ap.parse_args()

    load_env(args.env)
    uri, user, pwd = (os.environ.get(k) for k in ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"))
    db = os.environ.get("NEO4J_DATABASE", "neo4j")
    if not (uri and user and pwd):
        sys.exit(f"No credentials found. Save the Aura credentials file as {args.env} "
                 "(see aura.env.example) or set NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD.")

    try:
        driver = GraphDatabase.driver(uri, auth=(user, pwd))
    except ConfigurationError:
        sys.exit(f"NEO4J_URI {uri!r} looks wrong. For Aura it starts with neo4j+s:// "
                 "(copy it from the credentials file or the instance page in the Aura console).")
    try:
        driver.verify_connectivity()
    except AuthError:
        sys.exit("Connected, but the username/password was rejected. Check aura.env.")
    except (ServiceUnavailable, OSError, ValueError) as e:
        sys.exit(f"Could not reach {uri}: {e}\nIs the instance running (not paused) in the Aura console? "
                 "A brand-new instance can take a minute or two before it accepts connections.")
    print(f"Connected to {uri}")

    def run(query, **params):
        return driver.execute_query(query, database_=db, **params)

    t0 = time.time()
    if args.wipe:
        deleted = 0
        while True:
            recs, _, _ = run("MATCH (n) WITH n LIMIT 5000 DETACH DELETE n RETURN count(*) AS n")
            if recs[0]["n"] == 0:
                break
            deleted += recs[0]["n"]
        print(f"Wiped {deleted:,} nodes")

    for q in SCHEMA:
        run(q)
    print("Constraints and indexes ready")

    def load(label, query, rows):
        for b in chunks(rows):
            run(query, rows=b)
        print(f"  {label:<17} {len(rows):>7,}")

    lots = read("lots.csv")
    for r in lots:
        r["qty"] = num(r["qty"])
    for kind in KINDS:     
        load(f"Lot:{kind}", Q_LOTS % kind, [r for r in lots if r["kind"] == kind])
    load("Equipment", Q_EQUIPMENT, read("equipment.csv"))
    load("Location", Q_LOCATIONS, read("locations.csv"))

    used = read("used_in.csv")
    for r in used:
        r["qty"] = num(r["qty"])
        r["pieces"] = int(r["pieces"]) if r["pieces"] else None
    load("USED_IN", Q_USED_IN, used)
    proc = read("processed_on.csv")
    for r in proc:
        r["events"] = int(r["events"])
    load("PROCESSED_ON", Q_PROCESSED_ON, proc)
    stored = read("stored_at.csv")
    for r in stored:
        r["dew_point_c"] = num(r["dew_point_c"])
    load("STORED_AT", Q_STORED_AT, stored)

    n_in = 0
    for path in sorted(glob.glob(os.path.join(DATA, "cohorts", "scenario_*_*.csv"))):
        name = os.path.basename(path)[:-4]                 
        _, scenario, role = name.split("_")
        with open(path, newline="", encoding="utf-8") as f:
            ids = [r["unit_id"] for r in csv.DictReader(f)]
        run(Q_COHORT, name=name, scenario=int(scenario), role=role, ids=ids)
        n_in += len(ids)
    print(f"  {'IN_COHORT':<17} {n_in:>7,}")

    recs, _, _ = run(Q_COUNTS)
    nodes, rels = recs[0]["nodes"], recs[0]["relationships"]
    with open(os.path.join(HERE, "expected_answers.json"), encoding="utf-8") as f:
        exp = json.load(f)["counts"]
    ok = nodes == exp["total_nodes"] and rels == exp["total_relationships"]
    print(f"\nDatabase now holds {nodes:,} nodes and {rels:,} relationships "
          f"(expected {exp['total_nodes']:,} / {exp['total_relationships']:,}) -> "
          f"{'OK' if ok else 'MISMATCH (did the database already contain other data? try --wipe)'}")
    print(f"Done in {time.time() - t0:.0f} s")
    driver.close()


if __name__ == "__main__":
    main()
