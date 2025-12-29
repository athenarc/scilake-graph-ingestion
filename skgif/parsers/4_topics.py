"""
Neo4j APOC Loading Commands:

# Load topics
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/topics/topics.jsonl") YIELD value RETURN value',
    'MERGE (t:Topic {local_identifier: value.local_identifier}) SET t = value',
    {batchSize: 1000}
);

# Load identifiers
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/topics/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 1000}
);

# Load HAS_PID relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/topics/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Topic {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Create indexes
CREATE INDEX topic_id FOR (t:Topic) ON (t.local_identifier);
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
    input_dir = Path(f"{base_dir}/dump/topic")
    output_dir = Path(f"{base_dir}/to_load/topics")
    output_dir.mkdir(parents=True, exist_ok=True)

    topics_file = gzip.open(output_dir / "topics.jsonl.gz", "wt", encoding="utf-8")
    rel_file = gzip.open(output_dir / "relationships.jsonl.gz", "wt", encoding="utf-8")
    identifiers_file = gzip.open(output_dir / "identifiers.jsonl.gz", "wt", encoding="utf-8")
    
    topic_count = 0
    
    # Define topic fields
    topic_fields = [
        "local_identifier",
        "entity_type"
    ]

    for file in input_dir.glob("*.txt.gz"):
        with gzip.open(file, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    for topic in data.get("@graph", []):
                        topic_id = topic.get("local_identifier")
                        topic_count += 1
                        
                        # Build topic data
                        topic_data = {
                            field: topic.get(field) for field in topic_fields
                            if topic.get(field) is not None
                        }
                        
                        labels = topic.get("labels", {})
                        add_multilingual_fields(topic_data, labels, "label")
                        
                        # Store original data
                        topic_data["_data"] = json.dumps(clean_empty(topic))
                        topic_data = clean_empty(topic_data)
                        topics_file.write(json.dumps(topic_data) + "\n")

                        # Handle identifiers
                        if topic.get("identifiers"):
                            for identifier in topic.get("identifiers"):
                                identifier_data = {
                                    "local_identifier": f"{identifier.get('scheme')}:{identifier.get('value')}",
                                    "scheme": identifier.get("scheme"),
                                    "value": identifier.get("value")
                                }
                                identifier_data = clean_empty(identifier_data)
                                if identifier_data:
                                    identifiers_file.write(json.dumps(identifier_data) + "\n")
                                
                                rel = {
                                    "start": topic_id,
                                    "end": identifier_data["local_identifier"],
                                    "type": "HAS_PID",
                                    "scheme": identifier.get("scheme")
                                }
                                rel = clean_empty(rel)
                                if rel:
                                    rel_file.write(json.dumps(rel) + "\n")

                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {file.name}: {e}")

    topics_file.close()
    rel_file.close()
    identifiers_file.close()
    
    print(f"\n=== Processed {topic_count} topics ===")
    print("✅ Done. Output saved in:", output_dir)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python topics.py <base_dir>")
    else:
        process_files(sys.argv[1])
