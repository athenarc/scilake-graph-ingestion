// Compute positive mentions count for each product
// Counts the number of CITANCE relationships with 'Supporting' polarity pointing to each product
// Products with mentions but no supporting citations get 0
CALL apoc.periodic.iterate(
  "
  MATCH ()-[c:CITANCE]->(tgt:Product)
  RETURN tgt, sum(CASE WHEN c.polarity = 'Supporting' THEN 1 ELSE 0 END) AS posCount
  ",
  "
  SET tgt.repro_positive_mentions_count = posCount
  ",
  { batchSize: 100000, parallel: false }
);

// Compute Replication Confidence Index (RCI) for each product
// RCI formula: (positive + 0.5 * neutral - negative) / total
// Ranges from -1 (all refuting) to +1 (all supporting), with neutral citations weighted at 0.5
CALL apoc.periodic.iterate(
  "
  MATCH ()-[c:CITANCE]->(tgt:Product)
  RETURN
    tgt,
    sum(CASE WHEN c.polarity = 'Supporting' THEN 1 ELSE 0 END) AS pos,
    sum(CASE WHEN c.polarity = 'Neutral' THEN 1 ELSE 0 END) AS neu,
    sum(CASE WHEN c.polarity = 'Refuting' THEN 1 ELSE 0 END) AS neg,
    count(c) AS total
  ",
  "
  SET tgt.repro_rci =
    CASE
      WHEN total = 0 THEN 0.0
      ELSE (pos + 0.5 * neu - neg) * 1.0 / total
    END
  ",
  { batchSize: 100000, parallel: false }
);

// Compute Focused Replication Confidence Index (Focused RCI) for each product
// Similar to RCI but only considers citations with 'Comparison' intent
// This provides a more focused measure of replication confidence based on direct comparisons
CALL apoc.periodic.iterate(
  "
  MATCH ()-[c:CITANCE]->(tgt:Product)
  WHERE c.intent = 'Comparison'
  RETURN
    tgt,
    sum(CASE WHEN c.polarity = 'Supporting' THEN 1 ELSE 0 END) AS pos,
    sum(CASE WHEN c.polarity = 'Neutral' THEN 1 ELSE 0 END) AS neu,
    sum(CASE WHEN c.polarity = 'Refuting' THEN 1 ELSE 0 END) AS neg,
    count(c) AS total
  ",
  "
  SET tgt.repro_focused_rci =
    CASE
      WHEN total = 0 THEN 0.0
      ELSE (pos + 0.5 * neu - neg) * 1.0 / total
    END
  ",
  { batchSize: 100000, parallel: false }
);
