-- file:create_index.sql ln:11 expect:true
CREATE INDEX IF NOT EXISTS onek_unique1 ON onek USING btree(unique1 int4_ops)
