CREATE INDEX researchartifact_local_identifier_index
FOR (ra:ResearchArtifact)
ON (ra.local_identifier);

CALL apoc.periodic.iterate(
  '
  // Replace the filename with the space-specific file you want to load,
  // e.g. cancer_research_artifacts.jsonl.gz, neuro_research_artifacts.jsonl.gz, etc.
  CALL apoc.load.json("file:///import/artifacts.jsonl") YIELD value
  RETURN value
  ',
  '
  // 1. Match the Product node by DOI (source paper)
  MATCH (pPid:Pid {scheme: "doi", value: value.doi})<-[:HAS_PID]-(p:Product)

  // 2. Create / update the ResearchArtifact node
  MERGE (ra:ResearchArtifact {local_identifier: value.artifact.local_identifier})
    ON CREATE SET
      ra.label    = value.artifact.label,
      ra.type     = value.artifact.type,
      ra.licenses = value.artifact.licenses,
      ra.versions = value.artifact.versions,
      ra.urls     = value.artifact.urls
    ON MATCH SET
      ra.label    = coalesce(value.artifact.label, ra.label),
      ra.type     = coalesce(value.artifact.type, ra.type),
      ra.licenses = coalesce(value.artifact.licenses, ra.licenses),
      ra.versions = coalesce(value.artifact.versions, ra.versions),
      ra.urls     = coalesce(value.artifact.urls, ra.urls)

  // 3. Create / update the relation from Product to ResearchArtifact
  MERGE (p)-[r:USES_RESEARCH_ARTIFACT]->(ra)
    ON CREATE SET
      r.research_artifact_score = value.relation.research_artifact_score,
      r.owned                   = value.relation.owned,
      r.owned_percentage        = value.relation.owned_percentage,
      r.owned_score             = value.relation.owned_score,
      r.reused                  = value.relation.reused,
      r.reused_percentage       = value.relation.reused_percentage,
      r.reused_score            = value.relation.reused_score,
      r.mentions_count          = value.relation.mentions_count
    ON MATCH SET
      r.research_artifact_score = coalesce(value.relation.research_artifact_score, r.research_artifact_score),
      r.owned                   = coalesce(value.relation.owned, r.owned),
      r.owned_percentage        = coalesce(value.relation.owned_percentage, r.owned_percentage),
      r.owned_score             = coalesce(value.relation.owned_score, r.owned_score),
      r.reused                  = coalesce(value.relation.reused, r.reused),
      r.reused_percentage       = coalesce(value.relation.reused_percentage, r.reused_percentage),
      r.reused_score            = coalesce(value.relation.reused_score, r.reused_score),
      r.mentions_count          = coalesce(value.relation.mentions_count, r.mentions_count)

  RETURN count(*) AS rows
  ',
  {batchSize: 10000}
);
