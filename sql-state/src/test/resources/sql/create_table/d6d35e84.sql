-- file:create_index.sql ln:958 expect:true
CREATE TABLE dupindexcols AS
  SELECT unique1 as id, stringu2::text as f1 FROM tenk1
