-- file:create_index.sql ln:724 expect:true
CREATE UNIQUE INDEX func_index_index on func_index_heap ((f1 || f2) text_ops)
