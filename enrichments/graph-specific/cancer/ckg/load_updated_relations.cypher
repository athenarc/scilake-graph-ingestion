// Drug.json
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/updated_relations_by_source_label/Drug.json') YIELD value
  RETURN value
  ",
  "
  MATCH (src:`Drug` {id: value.sourceId})
  MATCH (tgt:Product {local_identifier: value.targetId})
  WHERE id(src) <> id(tgt)
  CALL apoc.create.relationship(src, value.relType, value.props, tgt) 
  YIELD rel
  SET rel.mapped = true
  RETURN rel
  ",
  {batchSize:100000, parallel:false}
);

// Disease.json
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/updated_relations_by_source_label/Disease.json') YIELD value
  RETURN value
  ",
  "
  MATCH (src:`Disease` {id: value.sourceId})
  MATCH (tgt:Product {local_identifier: value.targetId})
  WHERE id(src) <> id(tgt)
  CALL apoc.create.relationship(src, value.relType, value.props, tgt) 
  YIELD rel
  SET rel.mapped = true
  RETURN rel
  ",
  {batchSize:100000, parallel:false}
);

// Protein.json
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/updated_relations_by_source_label/Protein.json') YIELD value
  RETURN value
  ",
  "
  MATCH (src:`Protein` {id: value.sourceId})
  MATCH (tgt:Product {local_identifier: value.targetId})
  WHERE id(src) <> id(tgt)
  CALL apoc.create.relationship(src, value.relType, value.props, tgt) 
  YIELD rel
  SET rel.mapped = true
  RETURN rel
  ",
  {batchSize:100000, parallel:false}
);

// Tissue.json
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/updated_relations_by_source_label/Tissue.json') YIELD value
  RETURN value
  ",
  "
  MATCH (src:`Tissue` {id: value.sourceId})
  MATCH (tgt:Product {local_identifier: value.targetId})
  WHERE id(src) <> id(tgt)
  CALL apoc.create.relationship(src, value.relType, value.props, tgt) 
  YIELD rel
  SET rel.mapped = true
  RETURN rel
  ",
  {batchSize:100000, parallel:false}
);

// Cellular_component.json
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/updated_relations_by_source_label/Cellular_component.json') YIELD value
  RETURN value
  ",
  "
  MATCH (src:`Cellular_component` {id: value.sourceId})
  MATCH (tgt:Product {local_identifier: value.targetId})
  WHERE id(src) <> id(tgt)
  CALL apoc.create.relationship(src, value.relType, value.props, tgt) 
  YIELD rel
  SET rel.mapped = true
  RETURN rel
  ",
  {batchSize:100000, parallel:false}
);

// Functional_region.json
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/updated_relations_by_source_label/Functional_region.json') YIELD value
  RETURN value
  ",
  "
  MATCH (src:`Functional_region` {id: value.sourceId})
  MATCH (tgt:Product {local_identifier: value.targetId})
  WHERE id(src) <> id(tgt)
  CALL apoc.create.relationship(src, value.relType, value.props, tgt) 
  YIELD rel
  SET rel.mapped = true
  RETURN rel
  ",
  {batchSize:100000, parallel:false}
);

// GWAS_study.json
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/updated_relations_by_source_label/GWAS_study.json') YIELD value
  RETURN value
  ",
  "
  MATCH (src:`GWAS_study` {id: value.sourceId})
  MATCH (tgt:Product {local_identifier: value.targetId})
  WHERE id(src) <> id(tgt)
  CALL apoc.create.relationship(src, value.relType, value.props, tgt) 
  YIELD rel
  SET rel.mapped = true
  RETURN rel
  ",
  {batchSize:100000, parallel:false}
);

// Modified_protein.json
CALL apoc.periodic.iterate(
  "
  CALL apoc.load.json('file:///import/updated_relations_by_source_label/Modified_protein.json') YIELD value
  RETURN value
  ",
  "
  MATCH (src:`Modified_protein` {id: value.sourceId})
  MATCH (tgt:Product {local_identifier: value.targetId})
  WHERE id(src) <> id(tgt)
  CALL apoc.create.relationship(src, value.relType, value.props, tgt) 
  YIELD rel
  SET rel.mapped = true
  RETURN rel
  ",
  {batchSize:100000, parallel:false}
);
