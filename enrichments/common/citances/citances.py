import os
import pandas as pd
import numpy as np
import json
import glob
import gzip
import sys

# Set pandas display options to show full output without truncation
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

def extract_citation_data(results_array, citation_mentions_array):
    """
    Extract structured data from the numpy arrays in results and citation_mentions columns.
    
    Args:
        results_array: Numpy array containing results data
        citation_mentions_array: Numpy array containing citation mentions
    
    Returns:
        List of dictionaries with extracted citation data
    """
    citations = []
    
    try:
        # Extract the list from the numpy array
        if isinstance(results_array, np.ndarray) and len(results_array) > 0:
            results_list = results_array[0]  # Get the first (and usually only) element
        else:
            results_list = results_array
        
        # Process the results list
        if isinstance(results_list, list):
            for citation_tuple in results_list:
                if isinstance(citation_tuple, tuple) and len(citation_tuple) >= 2:
                    position, metadata = citation_tuple
                    
                    # Extract metadata
                    if isinstance(metadata, dict):
                        semantics = metadata.get('semantics', 'Unknown')
                        intent = metadata.get('intent', 'Unknown')
                        polarity = metadata.get('polarity', 'Unknown')
                        
                        # Extract scores if available
                        scores = metadata.get('scores', {})
                        semantics_scores = scores.get('semantics', [])
                        intent_scores = scores.get('intent', [])
                        polarity_scores = scores.get('polarity', [])
                        
                        # Convert numpy arrays to lists for JSON serialization
                        if isinstance(semantics_scores, np.ndarray):
                            semantics_scores = semantics_scores.tolist()
                        if isinstance(intent_scores, np.ndarray):
                            intent_scores = intent_scores.tolist()
                        if isinstance(polarity_scores, np.ndarray):
                            polarity_scores = polarity_scores.tolist()
                        
                        citation = {
                            'semantics': semantics,
                            'intent': intent,
                            'polarity': polarity,
                            'semantics_scores': semantics_scores if semantics_scores else [],
                            'intent_scores': intent_scores if intent_scores else [],
                            'polarity_scores': polarity_scores if polarity_scores else []
                        }
                        citations.append(citation)
        # print(citations)
    except Exception as e:
        print(f"Warning: Error processing citation data: {e}")
        # Create a fallback citation
        citations.append({
            'semantics': "Unknown",
            'intent': "Unknown",
            'polarity': "Unknown",
            'semantics_scores': [],
            'intent_scores': [],
            'polarity_scores': []
        })
    
    return citations

def process_single_file(file_path, space_files, output_dir):
    """
    Process a single parquet file and write relations to appropriate gzipped JSONL files.
    
    Args:
        file_path: Path to the parquet file
        space_files: Dictionary to track open file handles for each space
        output_dir: Output directory for JSONL files
    """
    try:
        print(f"Processing: {os.path.basename(file_path)}")
        df = pd.read_parquet(file_path, engine="pyarrow")

        # Get unique rows by citation_id
        df = df.drop_duplicates(subset=["citationid"], keep="first")
        # print(df.size)
        file_relations = 0
        
        for idx, row in df.iterrows():
            citation_id = row['citationid']
            source_doi = str(row['source_doi']).lower()
            dest_doi = str(row['dest_doi']).lower()
            spaces = str(row['spaces'])
            space_doi = str(row['space_doi']).lower()
            
            # Extract citation data
            citations = extract_citation_data(row['results'], row['citation_mentions'])

            # print(citation_id)
            # print(row['results'])
            # print(row['citation_mentions'])
            # print(citations)
            # print()

            # Create relation records - handle both cases in one block
            if citations:
                # If citations found, create one relation per citation
                for i, citation in enumerate(citations):
                    relation = {
                        'citation_id': f"{int(citation_id)}:{i}",
                        'source_doi': source_doi.lower(),
                        'dest_doi': dest_doi.lower(),
                        # 'spaces': spaces,
                        # 'space_doi': space_doi,
                        'semantics': citation['semantics'],
                        'intent': citation['intent'],
                        'polarity': citation['polarity'],
                        'semantics_scores': citation['semantics_scores'],
                        'intent_scores': citation['intent_scores'],
                        'polarity_scores': citation['polarity_scores']
                    }
                    
                    # Write to appropriate space file
                    if spaces not in space_files:
                        # Create new gzipped file for this space
                        safe_space_name = spaces.replace('/', '_').replace('\\', '_').replace(' ', '_')
                        filename = f"{safe_space_name}.jsonl.gz"
                        filepath = os.path.join(output_dir, filename)
                        space_files[spaces] = {
                            'file': gzip.open(filepath, 'wt', encoding='utf-8'),
                            'filename': filename,
                            'filepath': filepath,
                            'count': 0
                        }
                        print(f"  Created new file: {filename}")
                    
                    # Write relation to file
                    json.dump(relation, space_files[spaces]['file'], ensure_ascii=False)
                    space_files[spaces]['file'].write('\n')
                    space_files[spaces]['count'] += 1
                    file_relations += 1
            else:
                # If no citations extracted, create a single basic relation
                relation = {
                    'citation_id': f"{int(citation_id)}:0",
                    'source_doi': source_doi.lower(),
                    'dest_doi': dest_doi.lower(),
                    # 'spaces': spaces,
                    # 'space_doi': space_doi,
                    'semantics': "Unknown",
                    'intent': "Unknown",
                    'polarity': "Unknown",
                    'semantics_scores': [],
                    'intent_scores': [],
                    'polarity_scores': []
                }
                
                # Write to appropriate space file
                if spaces not in space_files:
                    safe_space_name = spaces.replace('/', '_').replace('\\', '_').replace(' ', '_')
                    filename = f"{safe_space_name}.jsonl.gz"
                    filepath = os.path.join(output_dir, filename)
                    space_files[spaces] = {
                        'file': gzip.open(filepath, 'wt', encoding='utf-8'),
                        'filename': filename,
                        'filepath': filepath,
                        'count': 0
                    }
                    print(f"  Created new file: {filename}")
                
                json.dump(relation, space_files[spaces]['file'], ensure_ascii=False)
                space_files[spaces]['file'].write('\n')
                space_files[spaces]['count'] += 1
                file_relations += 1
        
        print(f"  Processed {file_relations} relations from {len(df)} rows")
        
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def close_space_files(space_files):
    """Close all open space files."""
    for space, file_info in space_files.items():
        file_info['file'].close()
        print(f"Closed {file_info['filename']} with {file_info['count']} relations")

def process_parquet_files(directory, max_files=None, output_dir="jsonl_output"):
    """
    Process parquet files individually and write relations to gzipped JSONL files by space.
    
    Args:
        directory: Directory containing parquet files
        max_files: Maximum number of files to process (None for all)
        output_dir: Output directory for JSONL files
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Find parquet files
    pattern = os.path.join(directory, "*.snappy.parquet")
    parquet_files = sorted(glob.glob(pattern))
    
    print(f"Found {len(parquet_files)} parquet files")
    
    if max_files:
        parquet_files = parquet_files[:max_files]
        print(f"Processing first {max_files} files")
    
    space_files = {}  # Track open files for each space
    
    try:
        # Process each file individually
        for i, file_path in enumerate(parquet_files):
            print(f"\nFile {i+1}/{len(parquet_files)}")
            process_single_file(file_path, space_files, output_dir)
    
    finally:
        # Close all open files
        close_space_files(space_files)

# Example usage
if __name__ == "__main__":
    directory = "/data/ser-data/citance_evaluate_030925/"
    
    # Process files and create gzipped JSONL output by space
    print("Processing files with relation extraction...")
    process_parquet_files(directory, max_files=None, output_dir="/data/ser-data/citances_parsed/")
