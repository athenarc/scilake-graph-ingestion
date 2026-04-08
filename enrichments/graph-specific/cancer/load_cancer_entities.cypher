// Load cancer entities and link them to Products using APOC periodic iterate.
//
// Input JSONL (one JSON object per line), example:
// {
//   "remapped_id": "50|doi_dedup___::3a274f9f9cefea6e43dda63ba9eea360",
//   "id": "50|doi_________::3a274f9f9cefea6e43dda63ba9eea360",
//   "section_label": "introduction",
//   "section_title": "",
//   "entities": [
//     {
//       "entity": "gene",
//       "text": "myc",
//       "model": "SIRIS-Lab/AIObioEnts-core-pubmedbert-full",
//       "linking": [{"id":"NCBI:138691271","name":"Myc","source":"NCBI_Gene"}]
//     }
//   ]
// }
//
// Product mapping strategy:
// - Derive Product.local_identifier from remapped_id, same pattern used by other scripts.
// - This lets us link deduplicated enrichment ids to OpenAIRE Product identifiers.
//
// Entity types handled:
// - gene, disease, chemical, species, cellline
// Only existing nodes are matched; no new cancer entity nodes are created.

CREATE INDEX gene_synonyms_idx
IF NOT EXISTS
FOR (n:Gene)
ON (n.synonyms);

CREATE INDEX disease_local_identifier_idx
IF NOT EXISTS
FOR (n:Disease)
ON (n.id);

CREATE INDEX drug_id_idx
IF NOT EXISTS
FOR (n:Drug)
ON (n.id);

// CREATE INDEX species_local_identifier_idx
// IF NOT EXISTS
// FOR (n:Species)
// ON (n.id);

CREATE INDEX tissue_id_idx
IF NOT EXISTS
FOR (n:Tissue)
ON (n.id);

// --------------------------------------------------------------------
// Load and link mentions for rows that have a matching Product.
// --------------------------------------------------------------------
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/cancer.jsonl') YIELD value
  RETURN value
  ",
  "
  WITH value
  WITH value, split(value.remapped_id, \"|\") AS id_parts
  WITH value, id_parts[1] AS id_part
  WITH value, \"https://explore.openaire.eu/search/result?id=\" + id_part AS product_local_id

  MATCH (p:Product {local_identifier: product_local_id})

  UNWIND value.entities AS ent
  UNWIND ent.linking AS link

  CALL apoc.do.case(
    [
      ent.entity = 'gene',     'MATCH (n:Gene) WHERE $id IN coalesce(n.synonyms, []) RETURN n AS node LIMIT 1',
      ent.entity = 'disease',  'MATCH (n:Disease {id: $id}) RETURN n AS node',
      ent.entity = 'chemical', 'MATCH (n:Drug {id: $id}) RETURN n AS node',
      ent.entity = 'cellline', 'MATCH (n:Tissue {id: $id}) RETURN n AS node'
    ],
    'RETURN NULL AS node',
    {id: link.id}
  ) YIELD value AS branch

  WITH p, value, ent, branch.node AS node
  WHERE node IS NOT NULL

  MERGE (p)-[r:HAS_IN_TEXT_MENTION {
    text: ent.text,
    model: ent.model,
    section_label: coalesce(value.section_label, 'title_or_abstract'),
    section_title: coalesce(value.section_title, 'title_or_abstract')
  }]->(node)

  RETURN count(*) AS processed
  ",
  {batchSize: 1000}
);
