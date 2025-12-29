"""
Neo4j APOC Loading Commands:

# Load datasources
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/datasources/datasources.jsonl") YIELD value RETURN value',
    'MERGE (d:Datasource {local_identifier: value.local_identifier}) SET d = value',
    {batchSize: 1000}
);

# Load identifiers
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/datasources/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 1000}
);

# Load HAS_PID relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/datasources/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Datasource {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Create indexes
CREATE INDEX datasource_id FOR (d:Datasource) ON (d.local_identifier);
CREATE INDEX pid_id FOR (i:Pid) ON (i.local_identifier);
"""

import gzip
import json
import os
from pathlib import Path
import sys
try:
    from .utils import clean_empty
except ImportError:
    from utils import clean_empty

def process_files(base_dir):
    input_dir = Path(f"{base_dir}/dump/datasource")
    output_dir = Path(f"{base_dir}/to_load/datasources")
    output_dir.mkdir(parents=True, exist_ok=True)

    datasources_file = gzip.open(output_dir / "datasources.jsonl.gz", "wt", encoding="utf-8")
    rel_file = gzip.open(output_dir / "relationships.jsonl.gz", "wt", encoding="utf-8")
    identifiers_file = gzip.open(output_dir / "identifiers.jsonl.gz", "wt", encoding="utf-8")
    
    datasource_count = 0
    
    # Define datasource fields
    datasource_fields = [
        "local_identifier",
        "entity_type",
        "name",
        "data_source_classification",
        "research_product_types",
        "disciplines"
    ]

    for file in input_dir.glob("*.txt.gz"):
        with gzip.open(file, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    for ds in data.get("@graph", []):
                        ds_id = ds.get("local_identifier")
                        datasource_count += 1
                        
                        # Build datasource data
                        datasource_data = {
                            field: ds.get(field) for field in datasource_fields
                            if ds.get(field) is not None
                        }

                        # Handle policy/policies and nested fields (store as JSON string)
                        if ds.get("policies"):
                            datasource_data["policies"] = json.dumps(ds["policies"])
                        if ds.get("persistent_identity_systems"):
                            datasource_data["persistent_identity_systems"] = json.dumps(ds["persistent_identity_systems"])
                        if ds.get("audience"):
                            datasource_data["audience"] = json.dumps(ds["audience"])

                        # Store original data
                        datasource_data["_data"] = json.dumps(clean_empty(ds))
                        datasource_data = clean_empty(datasource_data)
                        datasources_file.write(json.dumps(datasource_data) + "\n")

                        # Handle identifiers
                        if ds.get("identifiers"):
                            for identifier in ds.get("identifiers"):
                                identifier_data = {
                                    "local_identifier": f"{identifier.get('scheme')}:{identifier.get('value')}",
                                    "scheme": identifier.get("scheme"),
                                    "value": identifier.get("value")
                                }
                                identifier_data = clean_empty(identifier_data)
                                if identifier_data:
                                    identifiers_file.write(json.dumps(identifier_data) + "\n")
                                
                                rel = {
                                    "start": ds_id,
                                    "end": identifier_data["local_identifier"],
                                    "type": "HAS_PID",
                                    "scheme": identifier.get("scheme")
                                }
                                rel = clean_empty(rel)
                                if rel:
                                    rel_file.write(json.dumps(rel) + "\n")

                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {file.name}: {e}")

    datasources_file.close()
    rel_file.close()
    identifiers_file.close()
    
    print(f"\n=== Processed {datasource_count} datasources ===")
    print("✅ Done. Output saved in:", output_dir)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python 5_datasources.py <base_dir>")
    else:
        process_files(sys.argv[1]) 