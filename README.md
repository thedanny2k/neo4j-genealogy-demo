Simple genealogy graph for Neo4j AuraDB Free

A small AI generated data set to demo. It comes with a python loader for putting generated data set into neo4j, a set of queries with expected answers, and a small FastAPI backend (health check plus ancestors/descendants endpoints).

Open http://localhost:8000/docs to try the endpoints 
GET /health/neo4j | Pass/fail and response time 
GET /neo4j/ancestors/{lot_id}?depth=N | Everything that went into a lot 
GET /neo4j/descendants/{lot_id}?depth=N | Everything a lot went into 


