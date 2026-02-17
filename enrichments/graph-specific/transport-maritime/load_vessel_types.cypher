// Load VesselType entities and link them to Products using APOC periodic iterate.
//
// Input JSONL (one JSON object per line), example:
// {
//   "oaireid": "50|doi_dedup___::9187c90cf07c73c95c6709c6d061c233",
//   "entities": {
//     "entity": "vesselType",
//     "linking": [
//       {"id":"34","name":"Offshore Supply Vessel","source":"VesselTypes"},
//       {"id":"Q1201871","name":"Offshore Supply Vessel","source":"Wikidata"}
//     ],
//     "model": "SIRIS-Lab/SciLake-Maritime-roberta-base",
//     "text": "offshore vessels"
//   },
//   "id": "50|od______9451::f7b988c285b703330d9b4ad548ab74f9",
//   "section_label": "results",
//   "section_title": "Discussion and conclusions"
// }
//
// oaireid is the Product.local_identifier we link to.
// We merge existing VesselType nodes by name, then enrich with id and local_identifier.
// We connect them with HAS_IN_TEXT_MENTION relationships carrying context.

CREATE INDEX vesseltype_local_identifier_idx
IF NOT EXISTS
FOR (n:VesselType)
ON (n.local_identifier);

CREATE INDEX vesseltype_name_idx
IF NOT EXISTS
FOR (n:VesselType)
ON (n.name);


CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/maritime.json') YIELD value
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
         WHEN 'vesselType'     THEN 'VesselType'
         ELSE 'Error'
       END AS label

  // --- Create typed entity node ---
  // Merge by name (to match existing nodes), then enrich with all properties from link
  // and set local_identifier to link.id
  WITH p, value, ent, link, label,
       apoc.map.merge(
         apoc.map.clean(link, [], []),
         {local_identifier: link.id}
       ) AS enriched_props
  CALL apoc.merge.node(
    [label],
    {local_identifier: link.id},
    enriched_props
  ) YIELD node

  // --- Create contextual mention relationship ---
  MERGE (p)-[r:HAS_IN_TEXT_MENTION {
    text: ent.text,
    model: ent.model
  }]->(node)

  RETURN count(*) AS processed
  ",
  {batchSize: 100000}
);

