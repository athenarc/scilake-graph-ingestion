CREATE INDEX technology_local_identifier_index
FOR (t:Technology)
ON (t.local_identifier);

CALL apoc.periodic.iterate(
  "CALL apoc.load.json('file:///import/techs.json') YIELD value RETURN value",
  "MATCH (p:Product {local_identifier: value.id})
   UNWIND value.techs AS tech
   WITH p, tech, toLower(replace(replace(tech, ' ', '_'), '/', '_')) AS slug
   MERGE (t:Technology {local_identifier: slug})
     ON CREATE SET t.name = tech
   MERGE (p)-[:HAS_TECHNOLOGY]->(t);",
  {batchSize: 10000, parallel: false}
)