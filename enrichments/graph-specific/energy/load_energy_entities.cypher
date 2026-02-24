// Load energy entities and link them to Products using APOC periodic iterate.
//
// Input JSONL (one JSON object per line), example:
// {
//   "oaireid": "50|doi_dedup___::a4cd9cfa0e190fd957df6f7ca28fe4ea",
//   "entities": [
//     {
//       "entity": "energytype",
//       "linking": [
//         {"id":"ENERGY_TYPE:123","name":"solar energy","source":"Energy-Gazetteer"},
//         {"id":"Q12345","name":"solar energy","source":"Wikidata"}
//       ],
//       "model": "Energy-Gazetteer",
//       "text": "solar"
//     }
//   ],
//   "id": "50|RECOLECTA___::f463355ce8278a33e952e3dc40630dba",
//   "section_label": "introduction",
//   "section_title": ". . Energy sources and storage"
// }
//
// oaireid is the Product.local_identifier we link to.
// We create typed entity nodes (EnergyType, EnergyStorage)
// and connect them with HAS_IN_TEXT_MENTION relationships.
//
// A second block loads the same entity structure from a JSONL where each line has:
//   rsNr (LegalDocument.local_identifier) and entities array.
// Entities are linked to LegalDocument instead of Product.

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
  CALL apoc.load.json('file:///import/energytype.json') YIELD value
  RETURN value
  ",
  "
  WITH value
  // --- Transform oaireid to Product.local_identifier (same pattern as GEO script) ---
  WITH value, split(value.oaireid, \"|\") AS id_parts
  WITH value, id_parts[1] AS id_part
  WITH value, \"https://explore.openaire.eu/search/result?id=\" + id_part AS product_local_id

  // --- Product node ---
  MATCH (p:Product {local_identifier: product_local_id})

  // --- Entities ---
  UNWIND value.entities AS ent
  UNWIND ent.linking AS link

  WITH p, value, ent, link,
       CASE ent.entity
         WHEN 'energytype'     THEN 'EnergyType'
         WHEN 'energystorage'  THEN 'EnergyStorage'
         ELSE 'EnergyEntity'
       END AS label

  // --- Create typed entity node ---
  // Use a typed MERGE on (label, local_identifier), and keep original id as a separate property.
  // Include all properties from link (id, name, source, etc.)
  CALL apoc.merge.node(
    [label],
    {local_identifier: link.id},
    apoc.map.clean(link, [], [])
  ) YIELD node

  // --- Create contextual mention relationship ---
  MERGE (p)-[r:HAS_IN_TEXT_MENTION {
    text: ent.text,
    model: ent.model,
  }]->(node)

  RETURN count(*) AS processed
  ",
  {batchSize: 100000}
);

// --- Load energy entities linked to LegalDocument (rsNr = LegalDocument.local_identifier) ---
// Input JSONL: each line has rsNr and entities (same shape as above).
// Place file in import dir, e.g. file:///legal-energy-entities.jsonl
CALL apoc.periodic.iterate(
  '
  CALL apoc.load.json("file:///import/legal-energy-entities.jsonl") YIELD value
  RETURN value
  ',
  '
  WITH value
  WHERE value.rsNr IS NOT NULL AND value.entities IS NOT NULL AND size(value.entities) > 0
  MATCH (d:LegalDocument {local_identifier: value.rsNr})

  UNWIND value.entities AS ent
  UNWIND ent.linking AS link

  WITH d, value, ent, link,
       CASE ent.entity
         WHEN ''energytype''     THEN ''EnergyType''
         WHEN ''energystorage''  THEN ''EnergyStorage''
         ELSE ''EnergyEntity''
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
  ',
  {batchSize: 100000}
);

