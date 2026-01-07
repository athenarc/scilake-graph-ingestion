#!/bin/bash

for folder in energy-planning cancer-research ccam maritime neuroscience ebrains; do
    BASE_DIR="/data/tmp/skgif_dumps/${folder}"
    # python3 src/1_agents.py "$BASE_DIR"
    # python3 src/2_grants.py "$BASE_DIR"
    # python3 src/3_venues.py "$BASE_DIR"
    # python3 src/4_topics.py "$BASE_DIR"
    # python3 src/5_datasources.py "$BASE_DIR"
    python3 src/6_products.py "$BASE_DIR"
    echo "Processed $folder"
done
