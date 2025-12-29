CALL apoc.periodic.iterate(
  "
  MATCH (a)-[r:HAS_IN_TEXT_MENTION]->(b)
  RETURN a, b, count(r) AS weight
  ",
  "
  MERGE (a)-[m:MENTIONS]->(b)
  SET m.weight = weight
  ",
  {batchSize: 5000}
);