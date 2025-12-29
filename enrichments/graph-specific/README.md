# Graph-Specific Enrichments

This directory contains ETL scripts for loading domain-specific enrichments into the knowledge graph. These scripts load pre-extracted entities and link them to Product nodes, creating typed nodes with contextual relationships.

## Overview

Graph-specific enrichment loading scripts follow a common pattern:
- Load pre-extracted domain-specific entities (e.g., genes, geographic locations, techniques) from JSON/CSV files
- Create typed entity nodes with unique identifiers
- Link entities to Products via `HAS_IN_TEXT_MENTION` relationships with contextual metadata (section, position, text)

## Product ID Mapping

**Important:** Product IDs for all graph-specific enrichments need to be mapped using the following Zeppelin notebook:

https://iis-cdh5-test-gw.ocean.icm.edu.pl/zeppelin/#/notebook/2MCZXW6MH


## Domains

### Cancer (`cancer/`)
- **BCMO**: Loads gene relationships from CSV data (`load-bcmo-data.py`)
- **CKG**: Integrates Cancer Knowledge Graph data with product mappings

### Energy (`energy/`)
- **Geographic Entities**: Loads geographic locations (Wikidata/OSM) and links them to products
- **Energy Entities**: Loads energy types and storage entities
- **IRENA Entities**: Loads International Renewable Energy Agency entities

### Neuroscience (`neuro/`)
- **Neuro Entities**: Loads techniques, species, UBERON parcellations, biological sex, and preparation types
- **EBRAINS**: Merges EBRAINS knowledge graph data

### Transport CCAM (`transport-ccam/`)
- **CCAM**: Loads CCAM entities and links them to products

### Transport Maritime (`transport-maritime/`)
- **Maritime**: Loads vessel types and links them to maritime research products

## Usage

Most enrichments use Cypher scripts that can be executed in a graph database supporting Cypher (e.g., Neo4j or Avantgraph)

See individual domain directories for specific instructions.