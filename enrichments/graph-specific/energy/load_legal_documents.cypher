// Load LegalDocument entities from Fedlex dataset.
//
// Input: JSONL file at Neo4j import directory.
// Path: file:///fedlex-dataset-090425.jsonl (place file in import dir, e.g. /data2/neo4j/energy/import/)
//
// Properties in the JSONL (per-language: de, en, fr, it, rm):
//   rsNr                 - LOADED as local_identifier and as rsNr.
//   {lang}_abbreviation  - LOADED.
//   {lang}_lawTitle      - LOADED.
//   {lang}_url           - LOADED.
//   {lang}_dateApplicability - LOADED.
//   {lang}_lawHtml, {lang}_lawText - not loaded (very large).
//
// Only lightweight fields are stored. Properties are set only when not null.
// LegalDocument is not linked to Product.

CREATE INDEX legaldocument_local_identifier_idx
IF NOT EXISTS
FOR (n:LegalDocument)
ON (n.local_identifier);

CALL apoc.periodic.iterate(
  '
  CALL apoc.load.json("file:///import/fedlex-dataset-090425.jsonl") YIELD value
  RETURN value
  ',
  '
  WITH value
  WHERE value.rsNr IS NOT NULL

  MERGE (d:LegalDocument {local_identifier: value.rsNr})
  ON CREATE SET
    d.rsNr = value.rsNr,
    d.de_abbreviation = CASE WHEN value.de_abbreviation IS NOT NULL THEN value.de_abbreviation END,
    d.de_lawTitle     = CASE WHEN value.de_lawTitle IS NOT NULL THEN value.de_lawTitle END,
    d.de_url          = CASE WHEN value.de_url IS NOT NULL THEN value.de_url END,
    d.de_dateApplicability = CASE WHEN value.de_dateApplicability IS NOT NULL THEN value.de_dateApplicability END,
    d.en_abbreviation = CASE WHEN value.en_abbreviation IS NOT NULL THEN value.en_abbreviation END,
    d.en_lawTitle     = CASE WHEN value.en_lawTitle IS NOT NULL THEN value.en_lawTitle END,
    d.en_url          = CASE WHEN value.en_url IS NOT NULL THEN value.en_url END,
    d.en_dateApplicability = CASE WHEN value.en_dateApplicability IS NOT NULL THEN value.en_dateApplicability END,
    d.fr_abbreviation = CASE WHEN value.fr_abbreviation IS NOT NULL THEN value.fr_abbreviation END,
    d.fr_lawTitle     = CASE WHEN value.fr_lawTitle IS NOT NULL THEN value.fr_lawTitle END,
    d.fr_url          = CASE WHEN value.fr_url IS NOT NULL THEN value.fr_url END,
    d.fr_dateApplicability = CASE WHEN value.fr_dateApplicability IS NOT NULL THEN value.fr_dateApplicability END,
    d.it_abbreviation = CASE WHEN value.it_abbreviation IS NOT NULL THEN value.it_abbreviation END,
    d.it_lawTitle     = CASE WHEN value.it_lawTitle IS NOT NULL THEN value.it_lawTitle END,
    d.it_url          = CASE WHEN value.it_url IS NOT NULL THEN value.it_url END,
    d.it_dateApplicability = CASE WHEN value.it_dateApplicability IS NOT NULL THEN value.it_dateApplicability END,
    d.rm_abbreviation = CASE WHEN value.rm_abbreviation IS NOT NULL THEN value.rm_abbreviation END,
    d.rm_lawTitle     = CASE WHEN value.rm_lawTitle IS NOT NULL THEN value.rm_lawTitle END,
    d.rm_url          = CASE WHEN value.rm_url IS NOT NULL THEN value.rm_url END,
    d.rm_dateApplicability = CASE WHEN value.rm_dateApplicability IS NOT NULL THEN value.rm_dateApplicability END
  ON MATCH SET
    d.rsNr = coalesce(value.rsNr, d.rsNr),
    d.de_abbreviation = CASE WHEN value.de_abbreviation IS NOT NULL THEN value.de_abbreviation ELSE d.de_abbreviation END,
    d.de_lawTitle     = CASE WHEN value.de_lawTitle IS NOT NULL THEN value.de_lawTitle ELSE d.de_lawTitle END,
    d.de_url          = CASE WHEN value.de_url IS NOT NULL THEN value.de_url ELSE d.de_url END,
    d.de_dateApplicability = CASE WHEN value.de_dateApplicability IS NOT NULL THEN value.de_dateApplicability ELSE d.de_dateApplicability END,
    d.en_abbreviation = CASE WHEN value.en_abbreviation IS NOT NULL THEN value.en_abbreviation ELSE d.en_abbreviation END,
    d.en_lawTitle     = CASE WHEN value.en_lawTitle IS NOT NULL THEN value.en_lawTitle ELSE d.en_lawTitle END,
    d.en_url          = CASE WHEN value.en_url IS NOT NULL THEN value.en_url ELSE d.en_url END,
    d.en_dateApplicability = CASE WHEN value.en_dateApplicability IS NOT NULL THEN value.en_dateApplicability ELSE d.en_dateApplicability END,
    d.fr_abbreviation = CASE WHEN value.fr_abbreviation IS NOT NULL THEN value.fr_abbreviation ELSE d.fr_abbreviation END,
    d.fr_lawTitle     = CASE WHEN value.fr_lawTitle IS NOT NULL THEN value.fr_lawTitle ELSE d.fr_lawTitle END,
    d.fr_url          = CASE WHEN value.fr_url IS NOT NULL THEN value.fr_url ELSE d.fr_url END,
    d.fr_dateApplicability = CASE WHEN value.fr_dateApplicability IS NOT NULL THEN value.fr_dateApplicability ELSE d.fr_dateApplicability END,
    d.it_abbreviation = CASE WHEN value.it_abbreviation IS NOT NULL THEN value.it_abbreviation ELSE d.it_abbreviation END,
    d.it_lawTitle     = CASE WHEN value.it_lawTitle IS NOT NULL THEN value.it_lawTitle ELSE d.it_lawTitle END,
    d.it_url          = CASE WHEN value.it_url IS NOT NULL THEN value.it_url ELSE d.it_url END,
    d.it_dateApplicability = CASE WHEN value.it_dateApplicability IS NOT NULL THEN value.it_dateApplicability ELSE d.it_dateApplicability END,
    d.rm_abbreviation = CASE WHEN value.rm_abbreviation IS NOT NULL THEN value.rm_abbreviation ELSE d.rm_abbreviation END,
    d.rm_lawTitle     = CASE WHEN value.rm_lawTitle IS NOT NULL THEN value.rm_lawTitle ELSE d.rm_lawTitle END,
    d.rm_url          = CASE WHEN value.rm_url IS NOT NULL THEN value.rm_url ELSE d.rm_url END,
    d.rm_dateApplicability = CASE WHEN value.rm_dateApplicability IS NOT NULL THEN value.rm_dateApplicability ELSE d.rm_dateApplicability END

  RETURN 1 AS processed
  ',
  {batchSize: 10000}
);
