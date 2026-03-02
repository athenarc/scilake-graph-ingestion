CALL apoc.periodic.iterate(
  "
  MATCH (m:Manifestation)
  WHERE m.publication_date IS NOT NULL
    AND valueType(m.publication_date) = 'STRING NOT NULL'
  RETURN m
  ",
  "
  SET m.publication_date = date(m.publication_date)
  ",
  {batchSize:5000, parallel:false}
);

