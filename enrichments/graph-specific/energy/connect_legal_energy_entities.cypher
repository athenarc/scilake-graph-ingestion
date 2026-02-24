// Connect LegalDocument nodes to energy entities (EnergyType, EnergyStorage).
//
// Input: JSONL with one JSON object per line, e.g.:
//   { "rsNr": "12345", "entities": [
//     { "entity": "energytype", "linking": [
//       {"id":"ENERGY_TYPE:123","name":"solar energy","source":"Energy-Gazetteer"}
//     ], "model": "Energy-Gazetteer", "text": "solar" }
//   ]}
//
// rsNr must match LegalDocument.local_identifier (e.g. from load_legal_documents.cypher).
// Energy entity nodes are merged by link.id as local_identifier; relationships
// LegalDocument -[:HAS_IN_TEXT_MENTION { text, model }]-> EnergyType|EnergyStorage are created.
//
// Place file in import dir, e.g. file:///import/energyType_fedlex.jsonl

CREATE INDEX energytype_local_identifier_idx
IF NOT EXISTS
FOR (n:EnergyType)
ON (n.local_identifier);

CREATE INDEX energystorage_local_identifier_idx
IF NOT EXISTS
FOR (n:EnergyStorage)
ON (n.local_identifier);

CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json(\"file:///import/energyType_fedlex.jsonl\") YIELD value
  RETURN value
  ",
  "
  WITH value
  WHERE value.rsNr IS NOT NULL AND value.entities IS NOT NULL AND size(value.entities) > 0
  MATCH (d:LegalDocument {local_identifier: value.rsNr})

  UNWIND value.entities AS ent
  UNWIND ent.linking AS link

  WITH d, value, ent, link,
       CASE ent.entity
         WHEN 'energytype'     THEN 'EnergyType'
         WHEN 'energystorage'  THEN 'EnergyStorage'
         ELSE 'EnergyEntity'
       END AS label

  CALL apoc.merge.node(
    [label],
    {local_identifier: link.id},
    apoc.map.clean(link, [], [])
  ) YIELD node

  MERGE (d)-[r:HAS_IN_TEXT_MENTION {
    text: ent.text,
    model: ent.model
  }]->(node)

  RETURN count(*) AS processed
  ",
  {batchSize: 100000}
);
