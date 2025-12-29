# SKG-IF Ingestion

This directory contains parsers and loaders for the Scientific Knowledge Graphs Interoperability Framework (SKG-IF) data. SKGIF provides the core knowledge graph structure with entities and relationships describing research products and their relationships to other scholarly entities.

## Overview

These parsers process raw data dumps and transform them into structured JSONL files ready for loading into the knowledge graph. The output of these parsers is used to load the data into the graph database using the `load-all.cypher` script.

## Components

### Parsers (`parsers/`)

The parsers process domain-specific data dumps and generate standardized JSONL output:

1. **`1_agents.py`**: Processes agent entities (authors, organizations)
   - Outputs: `agents.jsonl.gz`, `identifiers.jsonl.gz`, `relationships.jsonl.gz`
   - Entities: `Agent` nodes with affiliations

2. **`2_grants.py`**: Processes grant/funding information
   - Outputs: `grants.jsonl.gz`, `identifiers.jsonl.gz`, `relationships.jsonl.gz`
   - Entities: `Grant` nodes

3. **`3_venues.py`**: Processes publication venues
   - Outputs: `venues.jsonl.gz`, `identifiers.jsonl.gz`, `relationships.jsonl.gz`
   - Entities: `Venue` nodes

4. **`4_topics.py`**: Processes research topics/subjects
   - Outputs: `topics.jsonl.gz`, `identifiers.jsonl.gz`, `relationships.jsonl.gz`
   - Entities: `Topic` nodes

5. **`5_datasources.py`**: Processes data sources
   - Outputs: `datasources.jsonl.gz`, `identifiers.jsonl.gz`, `relationships.jsonl.gz`
   - Entities: `Datasource` nodes

6. **`6_products.py`**: Processes research products (publications, datasets, etc.)
   - Outputs: `products.jsonl.gz`, `identifiers.jsonl.gz`, `manifestations.jsonl.gz`, `relationships.jsonl.gz`
   - Entities: `Product` and `Manifestation` nodes

### Loader (`load-all.cypher`)

Cypher script that loads all SKGIF entities and relationships into the graph database. It:
- Creates indexes for all entity types
- Loads entities, identifiers, and relationships
- Handles multiple relationship types (HAS_PID, HAS_CONTRIBUTED_TO, HAS_TOPIC, FUNDED_BY, etc.)

### Transform Script (`transform-all.sh`)

Batch processing script that runs all parsers across multiple research domains:
- energy-planning
- cancer-research
- ccam
- maritime
- neuroscience
- ebrains

## Usage

### Processing Data

1. **Transform raw data**:
   ```bash
   ./transform-all.sh
   ```
   Or process individual domains:
   ```bash
   python3 parsers/1_agents.py /data/tmp/skgif_dumps/{domain}
   ```

2. **Load into graph database**:
   Execute `load-all.cypher` in your Cypher-compatible graph database

### Data Structure

Each parser generates:
- **Entities file**: Node definitions with properties
- **Identifiers file**: PID (Persistent Identifier) nodes
- **Relationships file**: Relationship definitions between entities

All output files are compressed JSONL format (`.jsonl.gz`).

## Requirements

- Python 3.x
- Required Python packages: gzip, json, pathlib
- Graph database supporting Cypher (Neo4j, Avantgraph, etc.)
- APOC library for batch loading operations

