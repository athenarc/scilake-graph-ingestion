# Geographic Entities Loading Guide

## Data Structure

Your JSONL file contains geographic entities extracted from product sections:

```json
{
  "id": "50|07b5c0ccd4fe::32cd385837e439845d296a9f9e5e52ef",
  "section_title": "Introduction.",
  "section_label": "introduction",
  "entities": [
    {
      "entity": "geo",
      "text": "North America",
      "role": "Object of study",
      "start": 2645,
      "end": 2658,
      "osm": {
        "osm_type": "node",
        "osm_id": 36966063,
        "lat": 51.0000002,
        "lon": -109.0,
        "display_name": "North America",
        "boundingbox": [26.0000002, 76.0000002, -134.0, -84.0]
      },
      "wikidata": "Q49"
    }
  ]
}
```

## Entities

### 1. Product (Existing)
- **Label**: `Product`
- **Identifier**: `local_identifier` (from the `id` field)
- **Status**: Already exists in the graph

### 2. GeographicEntity (New)
- **Label**: `GeographicEntity`
- **Identifier**: `local_identifier` (uses `wikidata` ID)
- **Properties**:
  - `name`: The text found (e.g., "North America")
  - `display_name`: From OSM (e.g., "North America")
  - `wikidata`: Wikidata ID (e.g., "Q49")
  - `osm_type`: OSM type (e.g., "node", "relation")
  - `osm_id`: OSM ID
  - `lat`: Latitude
  - `lon`: Longitude
  - `boundingbox`: Bounding box coordinates

## Relationships

### HAS_GEO_ENTITY
- **Type**: `Product -[HAS_GEO_ENTITY]-> GeographicEntity`
- **Properties**:
  - `role`: Role of the entity (e.g., "Object of study")
  - `section_title`: Title of the section where found
  - `section_label`: Label of the section (e.g., "introduction")
  - `start`: Character start position
  - `end`: Character end position
  - `text`: The text that was matched

## Loading Instructions

1. **Place your JSONL file** in the Neo4j import directory:
   ```
   /import/geo_entities.jsonl
   ```

2. **Run the Cypher script**:
   ```cypher
   :source /opt/bip-update-repo/bip-spaces/parsers/other-common/load_geo_entities.cypher
   ```
   
   Or copy and paste the commands from `load_geo_entities.cypher` into Neo4j Browser.

3. **The script will**:
   - Create `GeographicEntity` nodes from unique entities
   - Create indexes on `local_identifier` and `wikidata`
   - Create `HAS_GEO_ENTITY` relationships between Products and GeographicEntities

## Example Queries

### Find all geographic entities for a product:
```cypher
MATCH (p:Product {local_identifier: "50|07b5c0ccd4fe::32cd385837e439845d296a9f9e5e52ef"})
      -[r:HAS_GEO_ENTITY]->(g:GeographicEntity)
RETURN g.name, g.display_name, r.role, r.section_title
```

### Find all products mentioning a geographic entity:
```cypher
MATCH (p:Product)-[r:HAS_GEO_ENTITY]->(g:GeographicEntity {wikidata: "Q49"})
RETURN p.local_identifier, r.section_title, r.role
```

### Count geographic entities by role:
```cypher
MATCH (p:Product)-[r:HAS_GEO_ENTITY]->(g:GeographicEntity)
RETURN r.role, count(*) AS count
ORDER BY count DESC
```

