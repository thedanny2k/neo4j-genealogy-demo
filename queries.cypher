
// 1. Did everything load?
// expected: Lot 3049 (Cell 1484, Intermediate 1429, Package 50, RawMaterial 86),
//           Equipment 111, Location 185, Cohort 6
MATCH (n)
RETURN labels(n) AS labels, count(*) AS nodes
ORDER BY nodes DESC;

// expected: PROCESSED_ON 26192, USED_IN 11832, STORED_AT 7665, IN_COHORT 340
MATCH ()-[r]->()
RETURN type(r) AS relationship, count(*) AS count
ORDER BY count DESC;


// ---- 2. Look at one cell ---------------------------------------------------------------
// expected: one Cell node, product A, status SHIPPED, eol_result PASS
MATCH (c:Cell {lot_id: 'A2611-000264'})
RETURN c;


// ---- 3. What went into it? (graph view) ------------------------------------------------
// expected: 25 nodes on screen (the cell plus 24 upstream lots), 7 hops at the deepest
MATCH p = (:Lot)-[:USED_IN*1..]->(:Cell {lot_id: 'A2611-000264'})
RETURN p;

// Same question as a list: raw material lots only. expected: 13 rows
MATCH (raw:RawMaterial)-[:USED_IN*1..]->(:Cell {lot_id: 'A2611-000264'})
RETURN DISTINCT raw.lot_id AS lot, raw.lot_type AS type, raw.supplier AS supplier
ORDER BY lot;


// ---- 4. Where was it made and stored? (graph view) --------------------------------------
// expected: the cell plus the equipment and locations it touched
MATCH p = (:Cell {lot_id: 'A2611-000264'})-[:PROCESSED_ON|STORED_AT]->()
RETURN p;

// Its moves in time order.
// expected: FORM-ROOM:RACK-11, AGING-ROOM:RACK-11, BACKEND-STAGING, FG-WH:RACK-24
MATCH (:Cell {lot_id: 'A2611-000264'})-[s:STORED_AT]->(loc:Location)
RETURN s.ts AS at, loc.location_id AS location
ORDER BY at;


