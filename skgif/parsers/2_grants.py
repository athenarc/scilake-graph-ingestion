"""
Neo4j APOC Loading Commands:

# Load grants
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/grants.jsonl") YIELD value RETURN value',
    'MERGE (g:Grant {local_identifier: value.local_identifier}) SET g = value',
    {batchSize: 1000}
);

# Load identifiers
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 1000}
);

# Load HAS_PID relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Grant {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Load HAS_BENEFICIARY relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_BENEFICIARY"
     RETURN value',
    'MATCH (start:Grant {local_identifier: value.start})
     MATCH (end:Agent {local_identifier: value.end})
     MERGE (start)-[r:HAS_BENEFICIARY]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Load HAS_CONTRIBUTED_TO relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_CONTRIBUTED_TO"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Grant {local_identifier: value.end})
     MERGE (start)-[r:HAS_CONTRIBUTED_TO]->(end)
     SET r += value.properties
     RETURN r',
    {batchSize: 1000}
);

# Load HAS_FUNDING_AGENCY relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/grants/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_FUNDING_AGENCY"
     RETURN value',
    'MATCH (start:Grant {local_identifier: value.start})
     MATCH (end:Agent {local_identifier: value.end})
     MERGE (start)-[r:HAS_FUNDING_AGENCY]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Create indexes
CREATE INDEX grant_id FOR (g:Grant) ON (g.local_identifier);
CREATE INDEX pid_id FOR (i:Pid) ON (i.local_identifier);
"""

import gzip
import json
import os
from pathlib import Path
import sys
try:
    from .utils import add_multilingual_fields, clean_empty
except ImportError:  # script execution (no package)
    from utils import add_multilingual_fields, clean_empty

def process_files(base_dir):
    input_dir = Path(f"{base_dir}/dump/grants")
    output_dir = Path(f"{base_dir}/to_load/grants")
    output_dir.mkdir(parents=True, exist_ok=True)

    grants_file = gzip.open(output_dir / "grants.jsonl.gz", "wt", encoding="utf-8")
    rel_file = gzip.open(output_dir / "relationships.jsonl.gz", "wt", encoding="utf-8")
    identifiers_file = gzip.open(output_dir / "identifiers.jsonl.gz", "wt", encoding="utf-8")
    
    grant_count = 0
    
    # Define grant fields (excluding relationship fields and adding duration fields)
    grant_fields = [
        "local_identifier",
        "grant_number",
        "entity_type",
        "acronym",
        "funding_stream",
        "currency",
        "funded_amount",
        "keywords",
        "website"
    ]

    for file in input_dir.glob("*.txt.gz"):
        with gzip.open(file, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    for grant in data.get("@graph", []):
                        grant_id = grant.get("local_identifier")
                        grant_count += 1
                        
                        # Build grant data
                        grant_data = {
                            field: grant.get(field) for field in grant_fields
                            if grant.get(field) is not None
                        }
                        
                        # Handle multilingual titles
                        titles = grant.get("titles") or {}
                        add_multilingual_fields(grant_data, titles, "title")

                        # Handle multilingual abstracts
                        abstracts = grant.get("abstracts", {})
                        add_multilingual_fields(grant_data, abstracts, "abstract")

                        # Handle duration fields separately
                        if grant.get("duration"):
                            duration = grant.get("duration")
                            if duration.get("start"):
                                grant_data["duration_start"] = duration["start"]
                            if duration.get("end"):
                                grant_data["duration_end"] = duration["end"]

                        # Handle funding agency relationship
                        if grant.get("funding_agency"):
                            rel = {
                                "start": grant_id,
                                "end": grant["funding_agency"],
                                "type": "HAS_FUNDING_AGENCY"
                            }
                            rel = clean_empty(rel)
                            if rel:
                                rel_file.write(json.dumps(rel) + "\n")
                        
                        # Store original data
                        grant_data["_data"] = json.dumps(clean_empty(grant))
                        grant_data = clean_empty(grant_data)
                        grants_file.write(json.dumps(grant_data) + "\n")

                        # Handle identifiers
                        if grant.get("identifiers"):
                            for identifier in grant.get("identifiers"):
                                identifier_data = {
                                    "local_identifier": f"{identifier.get('scheme')}:{identifier.get('value')}",
                                    "scheme": identifier.get("scheme"),
                                    "value": identifier.get("value")
                                }
                                identifier_data = clean_empty(identifier_data)
                                if identifier_data:
                                    identifiers_file.write(json.dumps(identifier_data) + "\n")
                                
                                # Create HAS_PID relationship
                                rel = {
                                    "start": grant_id,
                                    "end": identifier_data["local_identifier"],
                                    "type": "HAS_PID",
                                    "scheme": identifier.get("scheme")
                                }
                                rel = clean_empty(rel)
                                if rel:
                                    rel_file.write(json.dumps(rel) + "\n")

                        # Handle beneficiaries
                        for beneficiary in grant.get("beneficiaries") or []:
                            rel = {
                                "start": grant_id,
                                "end": beneficiary,
                                "type": "HAS_BENEFICIARY"
                            }
                            rel = clean_empty(rel)
                            if rel:
                                rel_file.write(json.dumps(rel) + "\n")

                        # Handle contributions
                        for contribution in grant.get("contributions") or []:
                            rel = {
                                "start": contribution.get("by"),
                                "end": grant_id,
                                "type": "HAS_CONTRIBUTED_TO",
                                "properties": {
                                    "roles": contribution.get("roles"),
                                    "declared_affiliations": contribution.get("declared_affiliations")
                                }
                            }
                            rel = clean_empty(rel)
                            if rel:
                                rel_file.write(json.dumps(rel) + "\n")

                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {file.name}: {e}")

    grants_file.close()
    rel_file.close()
    identifiers_file.close()
    
    print(f"\n=== Processed {grant_count} grants ===")
    print("✅ Done. Output saved in:", output_dir)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python grants.py <base_dir>")
    else:
        process_files(sys.argv[1])
