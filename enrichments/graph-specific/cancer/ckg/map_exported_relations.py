import json
import gzip
import os

# Step 1: Load the mapping
matches_map = {}
with open("/data/tmp/skgif_dumps/cancer-research/to_load/matches_output.json", "r") as f:
    data = json.load(f)
    for entry in data:
        matches_map[str(entry["id"])] = entry["product_id"]

print(f"Loaded {len(matches_map)} ID→product_id mappings")

# Step 2: Process pub_relations.json line-by-line
with open("/data/tmp/skgif_dumps/cancer-research/to_load/pub_relations.json", "r") as fin:
    output_dir = "/data/tmp/skgif_dumps/cancer-research/to_load/updated_relations_by_source_label"
    os.makedirs(output_dir, exist_ok=True)
    writers = {}
    replaced_count = 0
    total_count = 0
    for line in fin:
        total_count += 1
        record = json.loads(line)
        tid = str(record.get("targetId"))
        if tid in matches_map:
            raw_labels = record.get("sourceLabels")
            if isinstance(raw_labels, list) and raw_labels:
                label_value = raw_labels[0]
                record["sourceLabels"] = label_value
            elif isinstance(raw_labels, str):
                label_value = raw_labels
            else:
                label_value = "unknown"
                record["sourceLabels"] = label_value

            record["targetId"] = matches_map[tid]

            safe_label = label_value
            if safe_label not in writers:
                out_path = os.path.join(output_dir, f"{safe_label}.json.gz")
                writers[safe_label] = gzip.open(out_path, "wt")
            json.dump(record, writers[safe_label])
            writers[safe_label].write("\n")
            replaced_count += 1

    for f in writers.values():
        f.close()
    print(f"Processed {total_count} lines, replaced {replaced_count} targetIds across {len(writers)} files in {output_dir}")
