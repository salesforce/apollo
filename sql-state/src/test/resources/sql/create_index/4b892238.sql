-- file:create_index.sql ln:23 expect:true
CREATE INDEX tenk1_unique2 ON tenk1 USING btree(unique2 int4_ops)
