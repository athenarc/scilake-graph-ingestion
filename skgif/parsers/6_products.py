"""
Neo4j APOC Loading Commands:

# Load products
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/products.jsonl") YIELD value RETURN value',
    'MERGE (p:Product {local_identifier: value.local_identifier}) SET p = value',
    {batchSize: 1000}
);

# Load identifiers
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/identifiers.jsonl") YIELD value RETURN value',
    'MERGE (i:Pid {local_identifier: value.local_identifier}) SET i = value',
    {batchSize: 1000}
);

# Load manifestations
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/manifestations.jsonl") YIELD value RETURN value',
    'MERGE (m:Manifestation {local_identifier: value.local_identifier}) SET m = value',
    {batchSize: 1000}
);

# Load HAS_PID relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_PID"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Pid {local_identifier: value.end})
     MERGE (start)-[r:HAS_PID]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Load HAS_CONTRIBUTED_TO relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_CONTRIBUTED_TO"
     RETURN value',
    'MATCH (start:Agent {local_identifier: value.start})
     MATCH (end:Product {local_identifier: value.end})
     MERGE (start)-[r:HAS_CONTRIBUTED_TO]->(end)
     SET r += value.properties
     RETURN r',
    {batchSize: 1000}
);

# Load HAS_TOPIC relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_TOPIC"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Topic {local_identifier: value.end})
     MERGE (start)-[r:HAS_TOPIC]->(end)
     SET r += value.properties
     RETURN r',
    {batchSize: 1000}
);

# Load HAS_MANIFESTATION relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HAS_MANIFESTATION"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Manifestation {local_identifier: value.end})
     MERGE (start)-[r:HAS_MANIFESTATION]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Load FUNDED_BY relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "FUNDED_BY"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Grant {local_identifier: value.end})
     MERGE (start)-[r:FUNDED_BY]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Load IS_RELEVANT_TO relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "IS_RELEVANT_TO"
     RETURN value',
    'MATCH (start:Product {local_identifier: value.start})
     MATCH (end:Agent {local_identifier: value.end})
     MERGE (start)-[r:IS_RELEVANT_TO]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Load HOSTED_BY relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "HOSTED_BY"
     RETURN value',
    'MATCH (start:Manifestation {local_identifier: value.start})
     MATCH (end:Datasource {local_identifier: value.end})
     MERGE (start)-[r:HOSTED_BY]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Load PUBLISHED_IN relationships
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
     WHERE value.type = "PUBLISHED_IN"
     RETURN value',
    'MATCH (start:Manifestation {local_identifier: value.start})
     MATCH (end:Venue {local_identifier: value.end})
     MERGE (start)-[r:PUBLISHED_IN]->(end)
     RETURN r',
    {batchSize: 1000}
);

# Load RELATED_PRODUCT relationships
CALL apoc.periodic.iterate(
  '
  CALL apoc.load.json("file:///import/products/relationships.jsonl") YIELD value 
  WHERE value.rel_type = "RELATED_PRODUCT"
  RETURN value
  ',
  '
  MATCH (start:Product {local_identifier: value.start})
  MERGE (end:Product {local_identifier: value.end})
  ON CREATE SET end:EXTERNAL
  WITH start, end, value
  CALL apoc.create.relationship(start, value.type, {}, end) YIELD rel
  RETURN rel
  ',
  {batchSize: 1000}
);


# Create indexes
CREATE INDEX product_id FOR (p:Product) ON (p.local_identifier);
CREATE INDEX manifestation_id FOR (m:Manifestation) ON (m.local_identifier);
CREATE INDEX pid_id FOR (i:Pid) ON (i.local_identifier);
"""

import gzip
import json
import os
from pathlib import Path
import sys
import re
try:
    from .utils import add_multilingual_fields, clean_empty
except ImportError:  # script execution (no package)
    from utils import add_multilingual_fields, clean_empty

def camel_to_upper_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.upper()

def process_files(base_dir):
    input_dir = Path(f"{base_dir}/dump/product")
    output_dir = Path(f"{base_dir}/to_load/products")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Open output files as plain text (uncompressed) JSONL files
    products_file = open(output_dir / "products.jsonl", "w", encoding="utf-8")
    rel_file = open(output_dir / "relationships.jsonl", "w", encoding="utf-8")
    identifiers_file = open(output_dir / "identifiers.jsonl", "w", encoding="utf-8")
    manifestations_file = open(output_dir / "manifestations.jsonl", "w", encoding="utf-8")
    
    product_count = 0
    
    # Define product fields
    product_fields = [
        "local_identifier",
        "entity_type",
        "product_type"
    ]

    for file in input_dir.glob("*.txt.gz"):
        with gzip.open(file, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    for prod in data.get("@graph", []):
                        prod_id = prod.get("local_identifier")
                        product_count += 1
                        
                        # Build product data
                        product_data = {
                            field: prod.get(field) for field in product_fields
                            if prod.get(field) is not None
                        }

                        # Handle multilingual titles
                        titles = prod.get("titles") or {}
                        add_multilingual_fields(product_data, titles, "title")

                        # Handle multilingual abstracts
                        abstracts = prod.get("abstracts", {})
                        add_multilingual_fields(product_data, abstracts, "abstract")

                        # Handle RA metrics: map categories to <metric>_class and measures to numeric properties
                        for metric in (prod.get("ra_metrics") or []):
                            ra = metric.get("ra_metric") or {}
                            measure = ra.get("ra_measure")
                            category = ra.get("ra_category")
                            value_raw = ra.get("ra_value")

                            # Category mapping: extract class (e.g., "C5") and map to appropriate <metric>_class
                            if category and isinstance(category, dict):
                                cat_labels = category.get("labels") or {}
                                # Prefer English label; fallback to first
                                label_text = None
                                if isinstance(cat_labels, dict):
                                    label_text = cat_labels.get("en") or next(iter(cat_labels.values()), None)
                                if isinstance(label_text, str):
                                    cls = None
                                    # Look for pattern like "Class C5"
                                    import re as _re
                                    m = _re.search(r"Class\s+([A-Z]\d)", label_text)
                                    if m:
                                        cls = m.group(1)
                                    # Determine metric key from label text
                                    if "Popularity" in label_text and cls:
                                        product_data["popularity_class"] = cls
                                    elif "Influence-alt" in label_text and cls:
                                        product_data["citation_count_class"] = cls
                                    elif "Influence" in label_text and cls:
                                        product_data["influence_class"] = cls
                                    elif "Impulse" in label_text and cls:
                                        product_data["impulse_class"] = cls

                            # Measure mapping: set numeric value under popularity/influence/citation_count/impulse
                            if measure and isinstance(measure, dict):
                                labels = measure.get("labels") or {}
                                label_text = None
                                if isinstance(labels, dict):
                                    label_text = labels.get("en") or next(iter(labels.values()), None)
                                metric_key = None
                                if isinstance(label_text, str):
                                    if "Popularity" in label_text:
                                        metric_key = "popularity"
                                    elif "Influence-alt" in label_text:
                                        metric_key = "citation_count"
                                    elif "Influence" in label_text:
                                        metric_key = "influence"
                                    elif "Impulse" in label_text:
                                        metric_key = "impulse"
                                if metric_key and value_raw is not None:
                                    # Convert scientific-string to float when possible
                                    try:
                                        product_data[metric_key] = float(value_raw)
                                    except (TypeError, ValueError):
                                        # If not numeric, keep raw
                                        product_data[metric_key] = value_raw

                        # Normalise British spelling to American spelling
                        if "relevant_organisations" in prod and "relevant_organizations" not in prod:
                            prod["relevant_organizations"] = prod["relevant_organisations"]

                        # Store original data
                        product_data["_data"] = json.dumps(clean_empty(prod))
                        product_data = clean_empty(product_data)
                        products_file.write(json.dumps(product_data) + "\n")

                        # Handle identifiers
                        if prod.get("identifiers"):
                            for identifier in prod.get("identifiers"):
                                identifier_data = {
                                    "local_identifier": f"{identifier.get('scheme')}:{identifier.get('value')}",
                                    "scheme": identifier.get("scheme"),
                                    "value": identifier.get("value")
                                }
                                identifier_data = clean_empty(identifier_data)
                                if identifier_data:
                                    identifiers_file.write(json.dumps(identifier_data) + "\n")
                                
                                rel = {
                                    "start": prod_id,
                                    "end": identifier_data["local_identifier"],
                                    "type": "HAS_PID",
                                    "scheme": identifier.get("scheme")
                                }
                                rel = clean_empty(rel)
                                if rel:
                                    rel_file.write(json.dumps(rel) + "\n")

                        # Handle topics
                        for topic in prod.get("topics") or []:
                            rel = {
                                "start": prod_id,
                                "end": topic.get("term"),
                                "type": "HAS_TOPIC",
                                "properties": {
                                    "provenance": json.dumps(topic.get("provenance"))
                                }
                            }
                            rel = clean_empty(rel)
                            if rel:
                                rel_file.write(json.dumps(rel) + "\n")

                        # Handle contributions
                        for contrib in prod.get("contributions") or []:
                            rel = {
                                "start": contrib.get("by"),
                                "end": prod_id,
                                "type": "HAS_CONTRIBUTED_TO",
                                "properties": {
                                    "role": contrib.get("role"),
                                    "declared_affiliations": contrib.get("declared_affiliations"),
                                    "rank": contrib.get("rank"),
                                    "contribution_types": contrib.get("contribution_types")
                                }
                            }
                            rel = clean_empty(rel)
                            if rel:
                                rel_file.write(json.dumps(rel) + "\n")

                        # Handle manifestations as separate entities
                        for idx, manif in enumerate(prod.get("manifestations") or []):
                            manif_id = f"{prod_id}:manifestation:{idx}"
                            manif_data = {}
                            manif_data["local_identifier"] = manif_id
                            # Flatten top-level fields
                            # manif_data["product_id"] = prod_id
                            manif_data["version"] = manif.get("version")
                            manif_data["licence"] = manif.get("licence")
                            # Flatten type
                            if manif.get("type"):
                                manif_type = manif["type"]
                                manif_data["type_class"] = manif_type.get("class") or None
                                manif_data["type_defined_in"] = manif_type.get("defined_in") or None
                                labels = manif_type.get("labels")
                                if labels:
                                    if "eng" in labels:
                                        manif_data["type_label"] = labels["eng"]
                                    elif "en" in labels:
                                        manif_data["type_label"] = labels["en"]
                                    else:
                                        manif_data["type_label"] = next(iter(labels.values()))
                                else:
                                    manif_data["type_label"] = None

                            # Flatten dates
                            if manif.get("dates"):
                                dates = manif["dates"]
                                for key in [
                                    "acceptance", "collected", "correction", "creation", "deposit", "embargo", "modified", "publication", "received", "retraction"
                                ]:
                                    if key in dates:
                                        val = dates[key]
                                        if isinstance(val, list) and val:
                                            manif_data[f"{key}_date"] = val[0]
                                        else:
                                            manif_data[f"{key}_date"] = val
                               
                            # Flatten peer_review
                            if manif.get("peer_review"):
                                pr = manif["peer_review"]
                                manif_data["peer_review_status"] = pr.get("status")
                                manif_data["peer_review_description"] = pr.get("description")

                            # Flatten access_rights
                            if manif.get("access_rights"):
                                ar = manif["access_rights"]
                                manif_data["access_rights_status"] = ar.get("status")
                                descriptions_value = ar.get("descriptions")
                                if descriptions_value is None and ar.get("description") is not None:
                                    descriptions_value = ar.get("description")
                                manif_data["access_rights_description"] = descriptions_value

                            # Flatten biblio
                            if manif.get("biblio"):
                                biblio = manif["biblio"]
                                # manif_data["biblio"] = json.dumps(biblio)
                                # Create HOSTED_BY relationship if hosting_data_source exists
                                if biblio.get("hosting_data_source"):
                                    hosted_by_rel = {
                                        "start": manif_id,
                                        "end": biblio["hosting_data_source"],
                                        "type": "HOSTED_BY"
                                    }
                                    hosted_by_rel = clean_empty(hosted_by_rel)
                                    if hosted_by_rel:
                                        rel_file.write(json.dumps(hosted_by_rel) + "\n")
                                # Create PUBLISHED_IN relationship if biblio.in (venue_id) exists
                                if biblio.get("in"):
                                    published_in_rel = {
                                        "start": manif_id,
                                        "end": biblio["in"],
                                        "type": "PUBLISHED_IN"
                                    }
                                    published_in_rel = clean_empty(published_in_rel)
                                    if published_in_rel:
                                        rel_file.write(json.dumps(published_in_rel) + "\n")

                            # Store original manifestation as _data
                            # manif_data["_data"] = json.dumps(clean_empty(manif))
                            manif_data = clean_empty(manif_data)
                            # Write manifestation entity
                            manifestations_file.write(json.dumps(manif_data) + "\n")
                            # Create HAS_MANIFESTATION relationship
                            rel = {
                                "start": prod_id,
                                "end": manif_id,
                                "type": "HAS_MANIFESTATION"
                            }
                            rel = clean_empty(rel)
                            if rel:
                                rel_file.write(json.dumps(rel) + "\n")
                            # Handle manifestation identifiers
                            for identifier in manif.get("identifiers") or []:
                                identifier_data = {
                                    "local_identifier": f"{identifier.get('scheme')}:{identifier.get('value')}",
                                    "scheme": identifier.get("scheme"),
                                    "value": identifier.get("value")
                                }
                                identifier_data = clean_empty(identifier_data)
                                if identifier_data:
                                    identifiers_file.write(json.dumps(identifier_data) + "\n")
                                rel = {
                                    "start": manif_id,
                                    "end": identifier_data["local_identifier"],
                                    "type": "HAS_PID",
                                    "scheme": identifier.get("scheme")
                                }
                                rel = clean_empty(rel)
                                if rel:
                                    rel_file.write(json.dumps(rel) + "\n")

                        # Handle relevant organisations
                        relevant_orgs = prod.get("relevant_organizations") or []
                        for org in relevant_orgs:
                            rel = {
                                "start": prod_id,
                                "end": org,
                                "type": "IS_RELEVANT_TO"
                            }
                            rel = clean_empty(rel)
                            if rel:
                                rel_file.write(json.dumps(rel) + "\n")

                        # Handle funding
                        for grant in prod.get("funding") or []:
                            rel = {
                                "start": prod_id,
                                "end": grant,
                                "type": "FUNDED_BY"
                            }
                            rel = clean_empty(rel)
                            if rel:
                                rel_file.write(json.dumps(rel) + "\n")

                        # Handle related products
                        related = prod.get("related_products", {}) or {}
                        for rel_type, targets in related.items():
                            for target in targets:
                                rel = {
                                    "start": prod_id,
                                    "end": target,
                                    "type": f"{camel_to_upper_snake(rel_type)}",
                                    "rel_type": "RELATED_PRODUCT"
                                }
                                rel = clean_empty(rel)
                                if rel:
                                    rel_file.write(json.dumps(rel) + "\n")

                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {file.name}: {e}")

    products_file.close()
    rel_file.close()
    identifiers_file.close()
    manifestations_file.close()
    
    print(f"\n=== Processed {product_count} products ===")
    print("✅ Done. Output saved in:", output_dir)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python 6_products.py <base_dir>")
    else:
        process_files(sys.argv[1]) 