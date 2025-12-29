# Merge CKG data with SKG-IF

## 1. Map publications to products

* Export publication pids

```
export publications.csv
CALL apoc.export.csv.query(
  "MATCH (p:Publication)
   RETURN p.id AS id,
          p.DOI AS doi,
          p.PMC_id AS pmcid",
  "file:///import/publications.csv",
  {}
)
YIELD file, nodes, relationships, properties, time
RETURN file, nodes, relationships, properties, time;
```

* Export research product pids

```
export products_pids.csv
CALL apoc.export.csv.query(
  "MATCH (n:Product)-[:HAS_PID]->(pid:Pid)
   RETURN n.local_identifier AS product_id, pid.scheme AS scheme, pid.value AS value",
  "file:///import/products_pids.csv",
  {}
);
```

* Move `publications.csv` and `products_pids.csv` in andrea

* Run mapping script

```
python3 map_pubs_to_products.py
```

* Move output file in spot under `/data/neo4j/cancer/import/` and load `SAME_AS` relations in Neo4j 

```
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/matches_output.json')
  YIELD value
  WITH value, toString(value.id) AS pubId
  WHERE value.product_id IS NOT NULL
  RETURN value, pubId
  ",
  "
  MATCH (pub:Publication {id: pubId})
  MATCH (prod:Product {local_identifier: value.product_id})
  WHERE id(pub) <> id(prod)
  MERGE (pub)-[:SAME_AS]->(prod)
  ",
  {batchSize:1000, parallel:false}
);
```

## 2. Copy relations that point to Publications to the corresponding Products

* Export all relations pointing to Publications; Publications do not have outgoing relations (apart from SAME_AS) - only incoming

```
CALL apoc.export.json.query(
    "MATCH (source)-[r]->(pub:Publication) RETURN labels(source) AS sourceLabels, source.id AS sourceId, pub.id AS targetId, type(r) AS relType, properties(r) AS props",
    "file:///import/pub_relations.json",
    {batchSize:200000, parallel:false, stream:false})
    YIELD file, nodes, relationships, properties, time
RETURN file;
```

* Join mapping with exported relations

```
python3 map_exported_relations.py
```


* Move mapped relations (output of previous script) to spot under /data/neo4j/cancer/import/

* Import relationships to Products

Use load_updated_relations.cypher