CREATE INDEX irena_type_local_id FOR (t:IrenaType) ON (t.local_identifier);

// Step 1: Create IrenaType nodes
CALL apoc.periodic.iterate(
  '
  CALL apoc.load.json("file:///import/irena.jsonl") YIELD value
  RETURN value
  ',
  '
  WITH value,
       toString(toInteger(value.id)) AS local_id
  MERGE (t:IrenaType {local_identifier: local_id})
  ON CREATE SET
    t.concept = value.concept,
    t.type = value.type,
    t.description = value.description,
    t.wikidata_id = value.wikidata_id,
    t.wikidata_aliases = value.wikidata_aliases
  ',
  {batchSize: 500}
);

// Step 2: Create parent-child relationships
CALL apoc.periodic.iterate(
  '
  CALL apoc.load.json("file:///import/irena.jsonl") YIELD value
  RETURN value
  ',
  '
  WITH toString(toInteger(value.id)) AS child_id, value.`parent-id` AS parent_raw
  WHERE parent_raw IS NOT NULL
  WITH child_id, toString(toInteger(parent_raw)) AS parent_id
  MATCH (child:IrenaType {local_identifier: child_id})
  MATCH (parent:IrenaType {local_identifier: parent_id})
  MERGE (child)-[:SUBCLASS_OF]->(parent)
  ',
  {batchSize: 500}
);


// map EnergyType and EnergyStorage to IrenaTypes
MATCH (i:IrenaType), (e:EnergyType)
WHERE i.local_identifier = e.local_identifier
MERGE (i)<-[:IS_TYPE_OF]-(e);

MATCH (i:IrenaType), (s:EnergyStorage)
WHERE i.local_identifier = s.local_identifier
MERGE (i)<-[:IS_TYPE_OF]-(s);
