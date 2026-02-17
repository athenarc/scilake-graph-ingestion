// Load CCAM (Connected, Cooperative and Automated Mobility) entities and link them to Products using APOC periodic iterate.
//
// Input JSONL (one JSON object per line), example:
// {
//   "id": "50|doi_________::c9da37491957e9f9d6eb76c8a3ce68fb",
//   "section_title": "Concluding Remarks",
//   "section_label": "introduction",
//   "entities": [
//     {
//       "entity": "scenariotype",
//       "text": "platooning",
//       "model": "SIRIS-Lab/SciLake-CCAM-roberta-large-other",
//       "linking": [
//         {"source":"SINFONICA-FAME","id":"SINFONICA::VehiclePlatooning","name":"Vehicle platooning"},
//         {"source":"Wikidata","id":"Q123689731","name":"Vehicle platooning"}
//       ]
//     },
//     {
//       "entity": "vehicletype",
//       "text": "CAVs",
//       "model": "SIRIS-Lab/SciLake-CCAM-roberta-large-vehicle-vru",
//       "linking": [
//         {"source":"SINFONICA-FAME","id":"SINFONICA::AutomatedVehicle","name":"Automated Vehicle"},
//         {"source":"Wikidata","id":"Q124535800","name":"Automated Vehicle"}
//       ]
//     }
//   ]
// }
//
// id is the Product.local_identifier we link to (format: "50|doi_________::<hash>").
// We create typed entity nodes (CommunicationType, EntityConnectionType, LevelOfAutomation, 
// ScenarioType, SensorType, VehicleType, VRUType) and connect them with HAS_IN_TEXT_MENTION relationships.

CREATE INDEX communicationtype_local_identifier_idx
IF NOT EXISTS
FOR (n:CommunicationType)
ON (n.local_identifier);

CREATE INDEX entityconnectiontype_local_identifier_idx
IF NOT EXISTS
FOR (n:EntityConnectionType)
ON (n.local_identifier);

CREATE INDEX levelofautomation_local_identifier_idx
IF NOT EXISTS
FOR (n:LevelOfAutomation)
ON (n.local_identifier);

CREATE INDEX scenariotype_local_identifier_idx
IF NOT EXISTS
FOR (n:ScenarioType)
ON (n.local_identifier);

CREATE INDEX sensortype_local_identifier_idx
IF NOT EXISTS
FOR (n:SensorType)
ON (n.local_identifier);

CREATE INDEX vehicletype_local_identifier_idx
IF NOT EXISTS
FOR (n:VehicleType)
ON (n.local_identifier);

CREATE INDEX vrutype_local_identifier_idx
IF NOT EXISTS
FOR (n:VRUType)
ON (n.local_identifier);

CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/ccam.json') YIELD value
  RETURN value
  ",
  "
  WITH value

  // --- Transform oaireid to Product.local_identifier ---
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
         WHEN 'communicationtype'      THEN 'CommunicationType'
         WHEN 'entityconnectiontype'  THEN 'EntityConnectionType'
         WHEN 'levelofautomation'     THEN 'LevelOfAutomation'
         WHEN 'scenariotype'          THEN 'ScenarioType'
         WHEN 'sensortype'            THEN 'SensorType'
         WHEN 'vehicletype'           THEN 'VehicleType'
         WHEN 'vrutype'               THEN 'VRUType'
         ELSE 'CCAMEntity'
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
