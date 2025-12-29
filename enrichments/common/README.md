# Common Enrichments

This directory contains ETL scripts for loading common enrichments that apply across all pilot graphs. These enrichments enhance the knowledge graph with cross-domain information extracted from products.

## Enrichments

### Artifacts (`artifacts/`)
- **Purpose**: Links research artifacts (datasets, software) to products
- **Scripts**:
  - `artifacts.py`: Python script to process and transform artifact data
  - `load-artifacts.cypher`: Cypher script to load artifacts into the graph
- **Entities**: `ResearchArtifact` nodes
- **Relationships**: `USES_RESEARCH_ARTIFACT` (Product → ResearchArtifact)
- **Properties**: Includes artifact metadata (label, type, licenses, versions, URLs) and usage metrics (scores, ownership, reuse statistics)

### Citances (`citances/`)
- **Purpose**: Creates extracted citation relationships with semantic annotations
- **Scripts**:
  - `citances.py`: Python script to extract and process citation data
  - `load-citances.cypher`: Cypher script to load citation relationships
- **Relationships**: `CITANCE` (Product → Product)
- **Properties**: Includes semantic information (semantics, intent, polarity) and associated scores

### Technologies (`technologies/`)
- **Purpose**: Extracts and links technology mentions from products
- **Scripts**:
  - `load-technologies.cypher`: Cypher script to load technology entities
- **Entities**: `Technology` nodes
- **Relationships**: `HAS_TECHNOLOGY` (Product → Technology)

### Text Mentions (`text-mentions/`)
- **Purpose**: Aggregates text mention relationships into weighted connections
- **Scripts**:
  - `create-mentions.cypher`: Cypher script to create aggregated mention relationships
- **Relationships**: `MENTIONS` (aggregated from `HAS_IN_TEXT_MENTION` with weights)

## Usage

Most enrichments use Cypher scripts that can be executed in a graph database supporting Cypher (e.g., Neo4j or Avantgraph). Python scripts require common Python packages (like pandas, numpy, etc.), as well as the data files in the expected format.
