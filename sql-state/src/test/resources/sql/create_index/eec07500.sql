-- file:create_index.sql ln:29 expect:true
CREATE INDEX tenk2_unique1 ON tenk2 USING btree(unique1 int4_ops)
