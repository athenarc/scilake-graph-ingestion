import pandas as pd
from neo4j import GraphDatabase
import sys

# === CONFIG ===
PRODUCTS_FILE = "/data/tmp/skgif_dumps/cancer-research/to_load/products_pids.csv"      # Format: product_id,scheme,value
PUBLICATIONS_FILE = "/data/tmp/skgif_dumps/cancer-research/to_load/publications.csv"   # Format: id,doi,pmcid
BATCH_SIZE = 1000

# === STEP 1: Load CSVs ===
products_df = pd.read_csv(PRODUCTS_FILE)
products_df.columns = ['product_id', 'scheme', 'value']

print(products_df.head())

publications_df = pd.read_csv(PUBLICATIONS_FILE)

print(publications_df.head())

# === STEP 2: Build in-memory lookup ===
pid_map = {'pmcid': {}, 'doi': {}}

for _, row in products_df.iterrows():
    if (row['scheme'] == 'pmcid' or row['scheme'] == 'doi'):
        pid_map[row['scheme'].lower()][row['value'].lower()] = row['product_id']

# === STEP 3: Match publications to products ===
matches = []

# Vectorized operations instead of iterrows()
publications_df['doi'] = publications_df['doi'].astype(str).str.strip()
publications_df['pmcid'] = publications_df['pmcid'].astype(str).str.strip()

# Create a function to find product_id and match type for a single row
def find_product_id_and_match_type(row):
    doi_lower = row['doi'].lower()
    pmcid_lower = row['pmcid'].lower()
    doi_original = row['doi']
    pmcid_original = row['pmcid']
    pub_id = row['id']
    
    # Check PMC first
    if pmcid_lower and pmcid_lower in pid_map['pmcid']:
        return pid_map['pmcid'][pmcid_lower], 'pmcid', pmcid_original, pub_id
    # If no PMC match, check DOI (case-insensitive)
    elif doi_lower and doi_lower in pid_map['doi']:
        return pid_map['doi'][doi_lower], 'doi', doi_original, pub_id
    return None, None, None, pub_id

# Apply the function to all rows at once with progress
print("Processing publications for matches...")
total_rows = len(publications_df)
results = []

for idx, row in publications_df.iterrows():
    if idx % 10 == 0 or idx == total_rows - 1:  # Print progress every 10 rows or on last row
        print(f"Processing row {idx + 1}/{total_rows}")
    
    product_id, match_type, match_value, pub_id = find_product_id_and_match_type(row)
    results.append({
        'id': pub_id,
        'doi': row['doi'],
        'pmcid': row['pmcid'],
        'product_id': product_id
    })

# Separate matches and missing publications
matches = [result for result in results if result['product_id'] is not None]
missing_publications = [result for result in results if result['product_id'] is None]

print(f"Total matches to create: {len(matches)}")
print(f"Total publications without matches: {len(missing_publications)}")

# Write matches to file
import json
with open('/data/ser-data/matches_output.json', 'w') as f:
    json.dump(matches, f, indent=2)
print(f"Matches written to matches_output.json")

# Also write missing publications as CSV for easier viewing
import csv
with open('/data/ser-data/missing_publications.csv', 'w', newline='') as f:
    if missing_publications:
        # Create CSV with only id, doi, pmcid
        csv_data = []
        for pub in missing_publications:
            csv_row = {
                'id': pub.get('id', ''),
                'doi': pub.get('doi', ''),
                'pmcid': pub.get('pmcid', '')
            }
            csv_data.append(csv_row)
        
        if csv_data:
            writer = csv.DictWriter(f, fieldnames=['id', 'doi', 'pmcid'])
            writer.writeheader()
            writer.writerows(csv_data)
        print(f"Missing publications also written to missing_publications.csv")

