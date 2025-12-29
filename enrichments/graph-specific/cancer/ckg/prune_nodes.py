# This script prunes the initial CKG and keeps only nodes that are not related to the disease mentioned in data/cancer_types.csv

import csv
import logging
import sys
import os
from neo4j import GraphDatabase
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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger()

def get_cancer_type_ids(csv_path='data/cancer_types.csv'):
    """Read cancer type IDs from CSV file
    
    Args:
        csv_path (str): Path to CSV file containing cancer types
        
    Returns:
        list: List of cancer type IDs
    """
    try:
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            cancer_ids = [row['id'] for row in reader]
            
        logger.info(f"Loaded {len(cancer_ids)} cancer type IDs")
        return cancer_ids
    except Exception as e:
        logger.error(f"Error loading cancer types from {csv_path}: {str(e)}")
        return []

# Example usage:
if __name__ == "__main__":
    disease_ids = get_cancer_type_ids()
    print(f"Found {len(disease_ids)} cancer types")
    # print("First few IDs:", disease_ids)

    # Initialize Neo4j driver
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:

        # # Set all nodes to Unreachable
        # set_all_unreachable = """
        # CALL apoc.periodic.iterate(
        #     "MATCH (n) RETURN n",
        #     "SET n:Unreachable",
        #     {batchSize: 10000, parallel: true}
        # ) YIELD batches, total
        # RETURN batches, total;
        # """
        # session.run(set_all_unreachable)
        # print("Set all nodes to Unreachable")

        # # Set all disease nodes to Reachable
        # mark_reachable_nodes = """
        # MATCH (n:Disease)
        # WHERE n.id IN $disease_ids
        # SET n:Reachable
        # REMOVE n:Unreachable
        # RETURN n
        # """
        # session.run(mark_reachable_nodes, disease_ids=disease_ids)
        # print("Marked disease nodes as Reachable")

        # IMPORTANT: execute until there are no Reachable nodes left 
        # check with MATCH (n:Reachable) RETURN count(n) -- in the neo4j browser
        # query = """
        # CALL apoc.periodic.iterate(
        #     "MATCH (n:Reachable) RETURN n",
        #     "OPTIONAL MATCH (n)--(u:Unreachable) SET u:Reachable REMOVE u:Unreachable REMOVE n:Reachable",
        #     {batchSize: 1000}
        # ) YIELD batches, total
        # RETURN batches, total;
        # """

        # Execute the query with a limit of 10000
        # session.run(query, limit=10000)
        session.run("MATCH path = (n:Reachable)-[*]-(u:Unreachable) RETURN path LIMIT 1;")

        print("Finished processing nodes.")
    driver.close()