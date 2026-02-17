// Load neuroscience entities and link them to Products using APOC periodic iterate.
//
// Input JSONL (one JSON object per line), example:
// {
//   "oaireid": "50|doi_dedup___::a4cd9cfa0e190fd957df6f7ca28fe4ea",
//   "entities": [
//     {
//       "entity": "technique",
//       "linking": [
//         {"id":"OPENMINDS:electronTomography","name":"electron tomography","source":"OPENMINDS-UBERON"},
//         {"id":"Q5358194","name":"electron tomography","source":"Wikidata"}
//       ],
//       "model": "Neuroscience-Gazetteer",
//       "text": "et"
//     }
//   ],
//   "id": "50|RECOLECTA___::f463355ce8278a33e952e3dc40630dba",
//   "section_label": "introduction",
//   "section_title": ". . Comparison of sensorimotor structures"
// }
//
// oaireid is the Product.local_identifier we link to.
// We create typed entity nodes (Technique, Species, UBERONParcellation, BiologicalSex, PreparationType)
// and connect them with MENTIONS relationships carrying context.

CREATE INDEX technique_local_identifier_idx
IF NOT EXISTS
FOR (n:Technique)
ON (n.local_identifier);

CREATE INDEX species_local_identifier_idx
IF NOT EXISTS
FOR (n:Species)
ON (n.local_identifier);

CREATE INDEX uberon_local_identifier_idx
IF NOT EXISTS
FOR (n:UBERONParcellation)
ON (n.local_identifier);

CREATE INDEX biologicalsex_local_identifier_idx
IF NOT EXISTS
FOR (n:BiologicalSex)
ON (n.local_identifier);

CREATE INDEX preparationtype_local_identifier_idx
IF NOT EXISTS
FOR (n:PreparationType)
ON (n.local_identifier);

CREATE INDEX neuroentity_local_identifier_idx
IF NOT EXISTS
FOR (n:NeuroEntity)
ON (n.local_identifier);

CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/neuroscience.json') YIELD value
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
         WHEN 'technique'          THEN 'Technique'
         WHEN 'species'            THEN 'Species'
         WHEN 'UBERONParcellation' THEN 'UBERONParcellation'
         WHEN 'biologicalSex'      THEN 'BiologicalSex'
         WHEN 'preparationType'    THEN 'PreparationType'
         ELSE 'NeuroEntity'
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
    section_label: value.section_label,
    section_title: value.section_title
  }]->(node)

  RETURN count(*) AS processed
  ",
  {batchSize: 1000}
);
