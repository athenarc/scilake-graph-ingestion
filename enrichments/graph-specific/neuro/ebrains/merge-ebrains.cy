// Merge ebrains Products with original Products by matching DOI (case-insensitive)
// 
// This script:
// 1. Finds all Products with source='ebrains'
// 2. Checks if they have a Pid with scheme='doi'
// 3. Finds the matching original Product (without source='ebrains') by DOI (case-insensitive)
// 4. Copies the Pid with scheme='ebrains' from ebrains Product to original Product (if it exists)
// 5. Tags the original Product with source: 'ebrains'
// 6. Deletes the ebrains Product
// 7. Deletes orphaned connected entities (if not connected to other Products)

CALL apoc.periodic.iterate(
  "
  // Find all ebrains Products that have a DOI Pid
  MATCH (ebrains_prod:Product {source: 'ebrains'})-[:HAS_PID]->(doi_pid:Pid {scheme: 'doi'})
  RETURN ebrains_prod, doi_pid
  ",
  "
  WITH ebrains_prod, doi_pid
  
  // Find the original Product (without source='ebrains') that has the same DOI (case-insensitive)
  MATCH (original_prod:Product)-[:HAS_PID]->(original_doi_pid:Pid {scheme: 'doi'})
  WHERE (original_prod.source <> 'ebrains' OR original_prod.source IS NULL)
    AND toLower(original_doi_pid.value) = toLower(doi_pid.value)
  
  // -------------------------------------------
  // 1) Copy the ebrains scheme Pid from ebrains Product to original Product (if it exists)
  // -------------------------------------------
  OPTIONAL MATCH (ebrains_prod)-[:HAS_PID]->(ebrains_pid:Pid {scheme: 'ebrains'})
  WITH ebrains_prod, original_prod, ebrains_pid
  // Copy ebrains Pid if it exists, otherwise proceed without it
  FOREACH (pid IN CASE WHEN ebrains_pid IS NOT NULL AND NOT (original_prod)-[:HAS_PID]->(ebrains_pid) THEN [ebrains_pid] ELSE [] END |
    MERGE (original_prod)-[:HAS_PID]->(pid)
  )
  
  // -------------------------------------------
  // 2) Tag the original Product with source: 'ebrains'
  // -------------------------------------------
  WITH ebrains_prod, original_prod
  SET original_prod.source = 'ebrains'
  
  // -------------------------------------------
  // 3) Collect all entities connected to the ebrains Product (for cleanup)
  // -------------------------------------------
  WITH ebrains_prod, original_prod
  OPTIONAL MATCH (ebrains_prod)-[rel]-(entity)
  WHERE entity <> original_prod AND NOT (entity:Product)  // Exclude original Product and other Products
  WITH ebrains_prod, original_prod, collect(DISTINCT entity) AS connected_entities
  
  // -------------------------------------------
  // 4) Delete the ebrains Product
  // -------------------------------------------
  WITH connected_entities, ebrains_prod
  DETACH DELETE ebrains_prod
  
  // -------------------------------------------
  // 5) Delete orphaned connected entities (if not connected to other Products)
  // -------------------------------------------
  WITH connected_entities
  UNWIND connected_entities AS entity
  WITH entity
  WHERE entity IS NOT NULL
  
  // Check if entity is connected to any other Product
  OPTIONAL MATCH (entity)--(other:Product)
  WITH entity, count(other) AS productConnections
  WHERE productConnections = 0
  
  DETACH DELETE entity
  
  RETURN count(entity) AS deleted_entities
  ",
  {batchSize: 1, parallel: false}
);
