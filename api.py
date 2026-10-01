"""
api.py - Neo4j side of the genealogy API (health check + ancestors/descendants).

    uvicorn api:app --reload        then open http://localhost:8000/docs

Endpoints
  GET /health/neo4j                          -> {"status": "pass"|"fail", ...}  (503 on fail)
  GET /neo4j/ancestors/{lot_id}?depth=N      -> everything that went INTO the lot
  GET /neo4j/descendants/{lot_id}?depth=N    -> everything the lot went INTO
  depth = number of USED_IN hops (1-50); leave it out for the full trace.

Trace response (agree this shape with the PostgreSQL endpoints so the UI can swap backends):
  {"root": "...", "direction": "ancestors", "depth": 2, "query_ms": 12.3,
   "nodes": [{"id": "...", "lot_type": "...", "kind": "Cell", "status": "SHIPPED"}, ...],
   "edges": [{"source": "<parent lot>", "target": "<child lot>", "step": "STACK"}, ...]}
"""
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from neo4j import GraphDatabase, RoutingControl

from load_to_aura import HERE, load_env

load_env(os.path.join(HERE, "aura.env"))
URI = os.environ.get("NEO4J_URI")
AUTH = (os.environ.get("NEO4J_USERNAME", "neo4j"), os.environ.get("NEO4J_PASSWORD", ""))
DB = os.environ.get("NEO4J_DATABASE", "neo4j")
if not URI:
    raise RuntimeError("NEO4J_URI not set: save your Aura credentials file as aura.env")

driver = GraphDatabase.driver(URI, auth=AUTH)   # lazy: connects on first query


@asynccontextmanager
async def lifespan(app):
    yield
    driver.close()


app = FastAPI(title="Genealogy API - Neo4j", lifespan=lifespan)
app.add_middleware(    # lets the Vite dev server (React + Plotly) call this API from the browser
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Variable-length bounds can't be query parameters in Cypher, so {hops} is filled in from a
# validated integer (FastAPI rejects anything that isn't 1-50 before we get here).
TRACE = """
MATCH (root:Lot {lot_id: $lot_id})
OPTIONAL MATCH %s
WITH root, collect(DISTINCT x) AS found
WITH [root] + found AS ns
UNWIND ns AS n
OPTIONAL MATCH (p:Lot)-[r:USED_IN]->(n)
WHERE p IN ns
WITH ns, collect(CASE WHEN r IS NULL THEN null
                      ELSE {source: p.lot_id, target: n.lot_id, step: r.step} END) AS edges
RETURN [x IN ns | {id: x.lot_id, lot_type: x.lot_type,
                   kind: head([l IN labels(x) WHERE l <> 'Lot']), status: x.status}] AS nodes,
       edges
"""
PATTERNS = {
    "ancestors": "(x:Lot)-[:USED_IN*1..{hops}]->(root)",
    "descendants": "(root)-[:USED_IN*1..{hops}]->(x:Lot)",
}


@app.get("/health/neo4j")
def health():
    t = time.perf_counter()
    try:
        # session.run is a single attempt (execute_query would retry for ~30 s before failing)
        with driver.session(database=DB) as session:
            session.run("RETURN 1 AS ok").consume()
    except Exception as e:     # any failure = database not usable right now
        return JSONResponse(status_code=503, content={
            "database": "neo4j", "status": "fail", "error": f"{type(e).__name__}: {e}"})
    return {"database": "neo4j", "status": "pass",
            "latency_ms": round((time.perf_counter() - t) * 1000, 1)}


def trace(lot_id: str, direction: str, depth: Optional[int]):
    query = TRACE % PATTERNS[direction].format(hops=depth if depth else "")
    t = time.perf_counter()
    records, _, _ = driver.execute_query(query, lot_id=lot_id, database_=DB,
                                         routing_=RoutingControl.READ)
    ms = round((time.perf_counter() - t) * 1000, 1)
    if not records:
        raise HTTPException(status_code=404, detail=f"lot {lot_id!r} not found")
    rec = records[0]
    return {"root": lot_id, "direction": direction, "depth": depth, "query_ms": ms,
            "nodes": rec["nodes"], "edges": rec["edges"]}


@app.get("/neo4j/ancestors/{lot_id}")
def ancestors(lot_id: str, depth: Optional[int] = Query(None, ge=1, le=50)):
    return trace(lot_id, "ancestors", depth)


@app.get("/neo4j/descendants/{lot_id}")
def descendants(lot_id: str, depth: Optional[int] = Query(None, ge=1, le=50)):
    return trace(lot_id, "descendants", depth)
