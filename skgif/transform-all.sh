#!/bin/bash

for folder in energy-planning cancer-research ccam maritime neuroscience ebrains; do
    BASE_DIR="/data/tmp/skgif_dumps/${folder}"
    python3 parsers/1_agents.py "$BASE_DIR"
    python3 parsers/2_grants.py "$BASE_DIR"
    python3 parsers/3_venues.py "$BASE_DIR"
    python3 parsers/4_topics.py "$BASE_DIR"
    python3 parsers/5_datasources.py "$BASE_DIR"
    python3 parsers/6_products.py "$BASE_DIR"
    echo "Processed $folder"
done
