CALL apoc.export.json.query(
  "MATCH (n) RETURN {id:id(n), labels:labels(n), properties:properties(n)} AS node",
  "file:///import/nodes.jsonl.gz",
  {jsonLines:true, compression:'GZIP'}
);


CALL apoc.export.json.query(
  "MATCH (a)-[r]->(b) RETURN {id:id(r), type:type(r), start_labels:labels(a), end_labels:labels(b), start:id(a), end:id(b), properties:properties(r)} AS rel",
  "file:///import/rels.jsonl.gz",
  {jsonLines:true, compression:'GZIP'}
);