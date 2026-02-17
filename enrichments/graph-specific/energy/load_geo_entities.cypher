// Load Geographic Entities into Neo4j
// 
// This script loads geographic entities extracted from product sections.
// The 'id' field in JSONL is transformed to match Product.local_identifier format:
//   Input:  "50|07b5c0ccd4fe::32cd385837e439845d296a9f9e5e52ef"
//   Output: "https://explore.openaire.eu/search/result?id=07b5c0ccd4fe::32cd385837e439845d296a9f9e5e52ef"
//
// ENTITIES:
// - Product (already exists, matched by local_identifier)
// - GeographicEntity (new nodes created from entities array)
//
// RELATIONSHIPS:
// - Product -[HAS_GEO_ENTITY]-> GeographicEntity
//   Properties: role, section_title, section_label, start, end, text

// Step 1: Create index on GeographicEntity
// Note: If index already exists, this command will fail - that's okay, just continue
// Index on local_identifier is used for MERGE operations in the loading script
CREATE INDEX geo_entity_id FOR (g:GeographicEntity) ON (g.local_identifier);

CALL apoc.periodic.iterate(
  '
  CALL apoc.load.json("file:///import/geoareas.jsonl2") YIELD value
  UNWIND value.entities AS entity

  // --- Transform oaireid → Product.local_identifier ---
  WITH value, entity, split(value.oaireid, "|") AS id_parts
  WITH value, entity, id_parts[1] AS id_part
  WITH
    "https://explore.openaire.eu/search/result?id=" + id_part AS product_local_id,
    value.section_title AS section_title,
    value.section_label AS section_label,
    entity

  RETURN product_local_id, section_title, section_label, entity
  ',
  '
  // --- Build stable Geographic identifier (Wikidata preferred, else OSM) ---
  WITH product_local_id, section_title, section_label, entity,
       COALESCE(
         "wikidata:" + toString(entity.wikidata),
         "osm:" + toString(entity.osm.osm_id)
       ) AS geo_identifier

  // --- Match Product ---
  MATCH (p:Product {local_identifier: product_local_id})

  // --- Create or Match GeographicEntity ---
  MERGE (g:GeographicEntity {local_identifier: geo_identifier})
  ON CREATE SET
    g.name = entity.text,
    g.wikidata = entity.wikidata,
    g.osm_id = entity.osm.osm_id,
    g.osm_type = entity.osm.osm_type,
    g.display_name = entity.osm.display_name,
    g.lat = entity.osm.lat,
    g.lon = entity.osm.lon,
    g.boundingbox = entity.osm.boundingbox

  // --- Create MENTIONS relationship (always create, never merge) ---
  CREATE (p)-[r:HAS_IN_TEXT_MENTION]->(g)
  SET
    r.role = entity.role,
    r.section_title = section_title,
    r.section_label = section_label,
    r.start = entity.start,
    r.end = entity.end,
    r.text = entity.text

  RETURN count(*) AS mentions_created
  ',
  {batchSize: 100000, parallel: true}
);
