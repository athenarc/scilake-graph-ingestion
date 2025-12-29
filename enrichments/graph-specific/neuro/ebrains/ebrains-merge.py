from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file in the same directory as this script
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

# Neo4j connection details
URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

if not all([URI, NEO4J_USER, NEO4J_PASSWORD]):
    raise ValueError("Missing required environment variables: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD")

AUTH = (NEO4J_USER, NEO4J_PASSWORD)
driver = GraphDatabase.driver(URI, auth=AUTH)

def get_products_with_same_doi(tx):
    """
    Returns groups of products sharing the same DOI (case-insensitive).
    Only returns groups that contain at least one product with source = 'ebrains'.
    """
    query = """
    MATCH (p:Product)-[:HAS_PID]->(pid:Pid {scheme:'doi'})
    WITH toLower(pid.value) AS doi, collect(p) AS products
    WHERE size(products) > 1
    AND ANY(p IN products WHERE p.source = 'ebrains')
    RETURN doi, products
    """
    return tx.run(query).data()

def pick_survivor(products):
    """
    Choose survivor according to these rules:
    1. Product WITHOUT source is required (ebrains products will be merged into non-source products)
    2. If still multiple → choose smallest local_identifier
    """
    no_source = [p for p in products if "source" not in p or p["source"] is None]
    if not no_source:
        raise ValueError("No product without source found in group - cannot merge")
    
    return min(no_source, key=lambda p: p["local_identifier"])

def merge_products(tx, survivor_id, duplicate_id):
    """
    Merges PIDs, then deletes duplicate product and orphaned nodes.
    """
    query = """
    MATCH (survivor:Product {local_identifier:$survivor_id})
    MATCH (dup:Product {local_identifier:$duplicate_id})

    // Copy PIDs: only schemes not already present on survivor
    OPTIONAL MATCH (dup)-[:HAS_PID]->(pid:Pid)
    WHERE pid IS NOT NULL AND NOT (survivor)-[:HAS_PID]->(:Pid {scheme: pid.scheme})
    WITH survivor, dup, collect(pid) AS pids_to_merge
    FOREACH (p IN pids_to_merge | MERGE (survivor)-[:HAS_PID]->(p))
    WITH survivor, dup

    // Survivor keeps its original source (should be None/null)

    // Collect duplicate's connected non-product entities
    OPTIONAL MATCH (dup)-[r]-(ent)
    WHERE NOT ent:Product
    WITH survivor, dup, collect(ent) AS ents

    // Delete duplicate product
    DETACH DELETE dup
    WITH ents

    // Remove orphaned entities
    UNWIND ents AS e
    OPTIONAL MATCH (e)--(other:Product)
    WITH e, count(other) AS c
    WHERE c = 0
    DETACH DELETE e
    """
    tx.run(query, survivor_id=survivor_id, duplicate_id=duplicate_id)

def main():
    with driver.session() as session:

        print("Fetching duplicate DOI groups...")
        grouped = session.execute_read(get_products_with_same_doi)

        for record in grouped:
            doi = record["doi"]
            products = record["products"]

            print(f"\nMerging DOI group: {doi}")
            for p in products:
                print("  - Product:", p["local_identifier"], "source:", p.get("source"))

            survivor = pick_survivor(products)

            print(f"  ✔ Survivor selected: {survivor['local_identifier']}")

            for p in products:
                # Only merge products with source = 'ebrains' into the survivor
                if (p["local_identifier"] != survivor["local_identifier"] 
                    and p.get("source") == "ebrains"):
                    print(f"    → Merging and deleting duplicate: {p['local_identifier']}")
                    session.execute_write(
                        merge_products,
                        survivor_id=survivor["local_identifier"],
                        duplicate_id=p["local_identifier"],
                    )

        print("\nAll duplicate DOI groups processed.")

if __name__ == "__main__":
    main()
