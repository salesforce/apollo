-- file:create_index.sql ln:21 expect:true
CREATE INDEX tenk1_unique1 ON tenk1 USING btree(unique1 int4_ops)
