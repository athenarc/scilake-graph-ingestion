from neo4j import GraphDatabase
import pandas as pd
import sys
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file in the same directory as this script
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

# Neo4j connection details
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
    raise ValueError("Missing required environment variables: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD")

# CSV file path
CSV_FILE = "data/bcmo_edited.csv"

# Load CSV data
df = pd.read_csv(CSV_FILE, index_col=0)

# replace . with _ in column/property names
df.columns = [col.replace(".", "_") for col in df.columns]

# Initialize Neo4j driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def create_gene_relationship(tx, source, target, properties):
    """
    Creates Gene nodes and a RELATED_TO relationship with properties.
    """
    query = """
    MATCH (src:Gene {id: $source})
    MATCH (tgt:Gene {id: $target})
    WHERE src IS NOT NULL AND tgt IS NOT NULL
    CREATE (src)-[r:RELATED_TO]->(tgt)
    SET r += $properties
    """
    
    # First, try to create the relationship
    result = tx.run(query, source=source, target=target, properties=properties)
    
    # If no rows are returned from the relationship creation, log the missing genes
    if not result.peek():

        # Log the query if either gene doesn't exist
        log_query = """
        MATCH (src:Gene {id: $source})
        MATCH (tgt:Gene {id: $target})
        WHERE src IS NULL OR tgt IS NULL
        RETURN 'Gene(s) not found: ' + COALESCE(src.id, 'None') + ' and ' + COALESCE(tgt.id, 'None') AS missing_genes
        """

        missing_result = tx.run(log_query, source=source, target=target)
        for record in missing_result:
            print(record['missing_genes'])

def import_data():
    with driver.session() as session:
        for _, row in df.iterrows():

            # Extract source and target symbols
            source = row["source_symbol"].upper()
            target = row["target_symbol"].upper()

            # Extract properties (all other statistical measures + provenance)
            properties = {col: row[col] for col in df.columns if col not in ["source_symbol", "target_symbol", "interaction", "name", "shared_interaction", "shared_name", "selected"]}
            
            # Convert properties to correct types (handle NaN)
            properties = {k: (v if pd.notna(v) else None) for k, v in properties.items()}

            # Create relationship in Neo4j
            session.write_transaction(create_gene_relationship, source, target, properties)

# Run the import process
import_data()

# Close driver connection
driver.close()

print("Data successfully imported into Neo4j.")
