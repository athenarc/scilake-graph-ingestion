# SciLake Graph Ingestion

This repository contains ETL scripts and loaders for building the SciLake Pilot knowledge graphs. The graphs are built in three layers: core structure, common enrichments, and pilot-specific enrichments.

## Architecture

SciLake Pilot graphs are constructed in a layered approach:

1. **Core Structure (SKG-IF)**: Foundation knowledge graph with core entities and relationships
2. **Common Enrichments**: Cross-domain enrichments applied to all pilot graphs
3. **Pilot-Specific Enrichments**: Domain-specific enrichments tailored to each research domain

## Components

### 1. Core: SKG-IF

The [SKGIF](skgif/) directory provides the foundational knowledge graph structure:
- **Core Entities**: Agents, Grants, Venues, Topics, Datasources, Products
- **Core Relationships**: HAS_PID, HAS_CONTRIBUTED_TO, HAS_TOPIC, FUNDED_BY, and more
- **Parsers**: Transform raw data dumps into structured JSONL files ready for loading

See [skgif/README.md](skgif/README.md) for details.

### 2. Common Enrichments

The [enrichments/common/](enrichments/common/) directory contains enrichments that apply to all pilot graphs:
- **Research Artifacts**: Links datasets and software to products
- **Citances**: Citation relationships with semantic annotations
- **Technologies**: Technology mentions extracted from products
- **Text Mentions**: Aggregated mention relationships

See [enrichments/common/README.md](enrichments/common/README.md) for details.

### 3. Pilot-Specific Enrichments

The [enrichments/graph-specific/](enrichments/graph-specific/) directory contains domain-specific enrichments for each pilot:

- **Cancer Research**: BCMO gene relationships, Cancer Knowledge Graph (CKG) integration
- **Energy Research**: Geographic entities, energy types, IRENA entities
- **Neuroscience**: Neuro entities (techniques, species, UBERON parcellations), EBRAINS integration
- **Transport-CCAM**: CCAM entities
- **Transport-Maritime**: Maritime vessel types

See [enrichments/graph-specific/README.md](enrichments/graph-specific/README.md) for details.

## Building a Pilot Graph

To build a complete pilot graph, follow this order:

1. **Load Core Structure**: Execute SKGIF parsers and loaders to create the foundation
2. **Apply Common Enrichments**: Load common enrichments that apply across all domains
3. **Apply Pilot-Specific Enrichments**: Load domain-specific enrichments for your pilot

## Usage

See individual README files for specific instructions and requirements.

