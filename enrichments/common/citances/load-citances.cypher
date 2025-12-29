CALL apoc.periodic.iterate(
  '
  CALL apoc.load.json("file:///import/citances.jsonl") YIELD value
  RETURN value
  ',
  '
  // Match source Product by DOI
  MATCH (srcPid:Pid {scheme: "doi", value: value.source_doi})<-[:HAS_PID]-(src:Product)
  // Match destination Product by DOI
  MATCH (dstPid:Pid {scheme: "doi", value: value.dest_doi})<-[:HAS_PID]-(dst:Product)

  // Create direct relationship with metadata
  CREATE (src)-[r:CITANCE {local_identifier: value.citation_id}]->(dst)
    SET r.semantics = value.semantics,
        r.intent = value.intent,
        r.polarity = value.polarity,
        r.semantics_scores = value.semantics_scores,
        r.intent_scores = value.intent_scores,
        r.polarity_scores = value.polarity_scores

  RETURN r
  ',
  {batchSize: 10000}
);