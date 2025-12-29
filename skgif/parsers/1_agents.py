"""
Neo4j APOC Loading Commands:

CALL apoc.periodic.iterate(
  'CALL apoc.load.json("file:///import/agents/agents.jsonl") YIELD value RETURN value',
  'MERGE (e:Agent {local_identifier: value.local_identifier}) SET e = value',
  {batchSize: 1000}
)

# Load identifiers
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/agents/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 1000}
);

# Load HAS_PID relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/agents/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 1000}
);

# IMPORTANT: NOT FOUND SUCH A RELATIONSHIP - Load AFFILIATED_WITH relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/agents/relationships.jsonl") YIELD value 
     WHERE value.type = "AFFILIATED_WITH"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Agent {local_identifier: value.end})
     MERGE (start)-[r:AFFILIATED_WITH]->(end)
     SET r += value
     REMOVE r.type, r.start, r.end
     RETURN r',
    {batchSize: 1000}
);

# Optional: Create indexes for better performance
CREATE INDEX agent_id FOR (e:Agent) ON (e.local_identifier);
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
    # Define input directory
    input_dir = Path(f"{base_dir}/dump/agent")
    
    # Define output directory
    output_dir = Path(f"{base_dir}/to_load/agents")

    output_dir.mkdir(parents=True, exist_ok=True)

    entities_file = gzip.open(output_dir / "agents.jsonl.gz", "wt", encoding="utf-8")
    rel_file = gzip.open(output_dir / "relationships.jsonl.gz", "wt", encoding="utf-8")
    identifiers_file = gzip.open(output_dir / "identifiers.jsonl.gz", "wt", encoding="utf-8")
    
    entity_type_counts = {}
    
    # Define all possible entity fields (removing identifiers)
    entity_fields = [
        "local_identifier",
        "entity_type", 
        "name", 
        "given_name",
        "family_name", 
        "short_name",
        "other_names",
        "website", 
        "country", 
        "types",
    ]

    print(f"\nProcessing directory: {input_dir}")
    if not input_dir.exists():
        print(f"Warning: Directory not found: {input_dir}")
        return
            
    for file in input_dir.glob("*.txt.gz"):
        # print(f"Processing file: {file}")
        with gzip.open(file, 'rt', encoding='utf-8') as f:
            for line_number, line in enumerate(f, start=1):
                try:
                    data = json.loads(line)

                    # CAUTION: this is a workaround to handle the case where @graph is not a list - in EBRAINS dataset
                    if not isinstance(data, dict):
                        continue
                    graph = data.get("@graph", [])
                    if isinstance(graph, dict):
                        graph = [graph]
                    elif not isinstance(graph, list):
                        continue

                    for entity in graph:
                        if not isinstance(entity, dict):
                            continue
                        # print(f"{file.name}:{line_number}: {entity}")
                        etype = entity.get("entity_type")
                        agent_id = entity.get("local_identifier")
                        entity_type_counts[etype] = entity_type_counts.get(etype, 0) + 1
                        
                        # Build entity data with original data as string
                        entity_data = {
                            field: entity.get(field) for field in entity_fields
                            if entity.get(field) is not None
                        }
                        # Clean and store the complete original entity as a JSON string
                        entity_data["_data"] = json.dumps(clean_empty(entity))
                        entity_data = clean_empty(entity_data)
                        entities_file.write(json.dumps(entity_data) + "\n")

                        # Handle identifiers
                        if entity.get("identifiers"):
                            for identifier in entity.get("identifiers"):
                                identifier_data = {
                                    "local_identifier": f"{identifier.get('scheme')}:{identifier.get('value')}",
                                    "scheme": identifier.get("scheme"),
                                    "value": identifier.get("value")
                                }
                                identifier_data = clean_empty(identifier_data)
                                if identifier_data:
                                    identifiers_file.write(json.dumps(identifier_data) + "\n")
                                
                                # Create relationship between entity and identifier
                                rel = {
                                    "start": agent_id,
                                    "end": identifier_data["local_identifier"],
                                    "type": "HAS_PID",
                                }
                                rel = clean_empty(rel)
                                if rel:
                                    rel_file.write(json.dumps(rel) + "\n")

                        # Handle affiliations relationships
                        if entity.get("affiliations"):
                            for aff in entity.get("affiliations"):
                                rel = {k: v for k, v in {
                                    "start": agent_id,
                                    "end": aff.get("affiliation"),
                                    "type": "AFFILIATED_WITH",
                                    "role": aff.get("role"),
                                    "period_start": aff.get("period", {}).get("start"),
                                    "period_end": aff.get("period", {}).get("end")
                                }.items() if v is not None}
                                rel = clean_empty(rel)
                                if rel:
                                    rel_file.write(json.dumps(rel) + "\n")

                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {file.name}: {e}")

    entities_file.close()
    rel_file.close()
    identifiers_file.close()
    
    # Print entity type report
    print("\n=== Entity Type Report ===")
    for etype, count in sorted(entity_type_counts.items()):
        print(f"{etype or 'None'}: {count}")
    print("=======================")
    print("✅ Done. Output saved in:", output_dir)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python agents.py <base_dir>")
    else:
        process_files(sys.argv[1])
