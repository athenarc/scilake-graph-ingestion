"""
Neo4j APOC Loading Commands:

# Load venues
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/venues/venues.jsonl") YIELD value RETURN value',
    'MERGE (v:Venue {local_identifier: value.local_identifier}) SET v = value',
    {batchSize: 1000}
);

# Load identifiers
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/venues/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 1000}
);

# Load HAS_PID relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/venues/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Venue {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Load HAS_CONTRIBUTED_TO relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/venues/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_CONTRIBUTED_TO"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Venue {local_identifier: value.end})
     MERGE (start)-[r:HAS_CONTRIBUTED_TO {role: value.properties.role}]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Create indexes
CREATE INDEX venue_id FOR (v:Venue) ON (v.local_identifier);
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
    input_dir = Path(f"{base_dir}/dump/venue")
    output_dir = Path(f"{base_dir}/to_load/venues")
    output_dir.mkdir(parents=True, exist_ok=True)

    venues_file = gzip.open(output_dir / "venues.jsonl.gz", "wt", encoding="utf-8")
    rel_file = gzip.open(output_dir / "relationships.jsonl.gz", "wt", encoding="utf-8")
    identifiers_file = gzip.open(output_dir / "identifiers.jsonl.gz", "wt", encoding="utf-8")
    
    venue_count = 0
    
    # Define venue fields
    venue_fields = [
        "local_identifier",
        "entity_type",
        "name",
        "acronym",
        "type",
        "series",
        "creation_date"
    ]

    for file in input_dir.glob("*.txt.gz"):
        # print(file)
        with gzip.open(file, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    for venue in data.get("@graph", []):
                        venue_id = venue.get("local_identifier")
                        venue_count += 1
                        
                        # Build venue data
                        venue_data = {
                            field: venue.get(field) for field in venue_fields
                            if venue.get(field) is not None
                        }
                        
                        # Handle flattened access_rights
                        if venue.get("access_rights"):
                            access_rights = venue.get("access_rights")
                            if access_rights.get("status"):
                                venue_data["access_rights_status"] = access_rights["status"]
                            if access_rights.get("description"):
                                venue_data["access_rights_description"] = access_rights["description"]
                        
                        # Store original data
                        venue_data["_data"] = json.dumps(clean_empty(venue))
                        venue_data = clean_empty(venue_data)
                        venues_file.write(json.dumps(venue_data) + "\n")

                        # Handle identifiers
                        if venue.get("identifiers"):
                            for identifier in venue.get("identifiers"):
                                identifier_data = {
                                    "local_identifier": f"{identifier.get('scheme')}:{identifier.get('value')}",
                                    "scheme": identifier.get("scheme"),
                                    "value": identifier.get("value")
                                }
                                identifier_data = clean_empty(identifier_data)
                                if identifier_data:
                                    identifiers_file.write(json.dumps(identifier_data) + "\n")
                                
                                rel = {
                                    "start": venue_id,
                                    "end": identifier_data["local_identifier"],
                                    "type": "HAS_PID",
                                    "scheme": identifier.get("scheme")
                                }
                                rel = clean_empty(rel)
                                if rel:
                                    rel_file.write(json.dumps(rel) + "\n")

                        # Handle contributions
                        for contribution in venue.get("contributions") or []:
                            rel = {
                                "start": contribution.get("by"),
                                "end": venue_id,
                                "type": "HAS_CONTRIBUTED_TO",
                                "properties": {
                                    "role": contribution.get("role")
                                }
                            }
                            rel = clean_empty(rel)
                            if rel:
                                rel_file.write(json.dumps(rel) + "\n")

                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {file.name}: {e}")

    venues_file.close()
    rel_file.close()
    identifiers_file.close()
    
    print(f"\n=== Processed {venue_count} venues ===")
    print("✅ Done. Output saved in:", output_dir)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python venues.py <base_dir>")
    else:
        process_files(sys.argv[1])
