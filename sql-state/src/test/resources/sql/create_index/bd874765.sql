-- file:create_index.sql ln:60 expect:true
CREATE INDEX bt_f8_index ON bt_f8_heap USING btree (seqno float8_ops)
