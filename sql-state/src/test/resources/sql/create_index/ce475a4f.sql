-- file:create_index.sql ln:9 expect:true
CREATE INDEX onek_unique1 ON onek USING btree(unique1 int4_ops)
