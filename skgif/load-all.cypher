
// DELETE DATA AND SCHEMA
//CALL apoc.periodic.iterate(
//  "MATCH (n) RETURN n",
//  "DETACH DELETE n",
//  {batchSize:10000, parallel:false}
//);

//CALL apoc.schema.assert({}, {});

// CREATE INDEXES
CREATE INDEX agent_id FOR (e:Agent) ON (e.local_identifier);
CREATE INDEX grant_id FOR (g:Grant) ON (g.local_identifier);
CREATE INDEX venue_id FOR (v:Venue) ON (v.local_identifier);
CREATE INDEX topic_id FOR (t:Topic) ON (t.local_identifier);
CREATE INDEX datasource_id FOR (d:Datasource) ON (d.local_identifier);
CREATE INDEX product_id FOR (p:Product) ON (p.local_identifier);
CREATE INDEX manifestation_id FOR (m:Manifestation) ON (m.local_identifier);
CREATE INDEX pid_id FOR (i:Pid) ON (i.local_identifier);

// AGENTS
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("file:///import/agents/agents.jsonl") YIELD value RETURN value',
  'MERGE (e:Agent {local_identifier: value.local_identifier}) SET e = value',
  {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/agents/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/agents/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/agents/relationships.jsonl") YIELD value 
     WHERE value.type = "AFFILIATED_WITH"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Agent {local_identifier: value.end})
     MERGE (start)-[r:AFFILIATED_WITH]->(end)
     SET r += value
     REMOVE r.type, r.start, r.end
     RETURN r',
    {batchSize: 20000}
);

// GRANTS
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/grants.jsonl") YIELD value RETURN value',
    'MERGE (g:Grant {local_identifier: value.local_identifier}) SET g = value',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Grant {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_BENEFICIARY"
     RETURN value',
    'MATCH (start:Grant {local_identifier: value.start})
     MATCH (end:Agent {local_identifier: value.end})
     MERGE (start)-[r:HAS_BENEFICIARY]->(end)
     RETURN r',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_CONTRIBUTED_TO"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Grant {local_identifier: value.end})
     MERGE (start)-[r:HAS_CONTRIBUTED_TO]->(end)
     SET r += value.properties
     RETURN r',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_FUNDING_AGENCY"
     RETURN value',
    'MATCH (start:Grant {local_identifier: value.start})
     MATCH (end:Agent {local_identifier: value.end})
     MERGE (start)-[r:HAS_FUNDING_AGENCY]->(end)
     RETURN r',
    {batchSize: 20000}
);

// VENUES
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/venues/venues.jsonl") YIELD value RETURN value',
    'MERGE (v:Venue {local_identifier: value.local_identifier}) SET v = value',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/venues/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/venues/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Venue {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/venues/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_CONTRIBUTED_TO"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Venue {local_identifier: value.end})
     MERGE (start)-[r:HAS_CONTRIBUTED_TO {role: value.properties.role}]->(end)
     RETURN r',
    {batchSize: 20000}
);

// TOPICS
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/topics/topics.jsonl") YIELD value RETURN value',
    'MERGE (t:Topic {local_identifier: value.local_identifier}) SET t = value',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/topics/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/topics/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Topic {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 20000}
);

// DATASOURCES
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/datasources/datasources.jsonl") YIELD value RETURN value',
    'MERGE (d:Datasource {local_identifier: value.local_identifier}) SET d = value',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/datasources/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 20000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/datasources/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Datasource {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 20000}
);

// PRODUCTS
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/products.jsonl") YIELD value RETURN value',
    'MERGE (p:Product {local_identifier: value.local_identifier}) SET p = value',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/manifestations.jsonl") YIELD value RETURN value',
    'MERGE (m:Manifestation {local_identifier: value.local_identifier}) SET m = value',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_CONTRIBUTED_TO"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Product {local_identifier: value.end})
     MERGE (start)-[r:HAS_CONTRIBUTED_TO]->(end)
     SET r += value.properties
     RETURN r',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_TOPIC"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Topic {local_identifier: value.end})
     MERGE (start)-[r:HAS_TOPIC]->(end)
     SET r += value.properties
     RETURN r',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_MANIFESTATION"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Manifestation {local_identifier: value.end})
     MERGE (start)-[r:HAS_MANIFESTATION]->(end)
     RETURN r',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "FUNDED_BY"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Grant {local_identifier: value.end})
     MERGE (start)-[r:FUNDED_BY]->(end)
     RETURN r',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HOSTED_BY"
     RETURN value',
    'MATCH (start:Manifestation {local_identifier: value.start})
     MATCH (end:Datasource {local_identifier: value.end})
     MERGE (start)-[r:HOSTED_BY]->(end)
     RETURN r',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "PUBLISHED_IN"
     RETURN value',
    'MATCH (start:Manifestation {local_identifier: value.start})
     MATCH (end:Venue {local_identifier: value.end})
     MERGE (start)-[r:PUBLISHED_IN]->(end)
     RETURN r',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "IS_RELEVANT_TO"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Agent {local_identifier: value.end})
     MERGE (start)-[r:IS_RELEVANT_TO]->(end)
     RETURN r',
    {batchSize: 10000}
);
CALL apoc.periodic.iterate(
  '
  CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
  WHERE value.rel_type = "RELATED_PRODUCT"
  RETURN value
  ',
  '
  MATCH (start:Product {local_identifier: value.start})
  MATCH (end:Product {local_identifier: value.end})
  WITH start, end, value
  CALL apoc.create.relationship(start, value.type, {}, end) YIELD rel
  RETURN rel
  ',
  {batchSize: 10000}
);
