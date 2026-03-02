
CALL apoc.periodic.iterate(
  "
  MATCH (p:Product)
  RETURN p
  ",
  "
  SET p.has_energy_type = EXISTS( (p)-[:MENTIONS]->(:EnergyType) ),
      p.has_energy_storage = EXISTS( (p)-[:MENTIONS]->(:EnergyStorage) )
  ",
  {batchSize:10000, parallel:false}
);


CREATE INDEX product_has_energy_type_idx IF NOT EXISTS
FOR (p:Product)
ON (p.has_energy_type);

CREATE INDEX product_has_energy_storage_idx IF NOT EXISTS
FOR (p:Product)
ON (p.has_energy_storage);
