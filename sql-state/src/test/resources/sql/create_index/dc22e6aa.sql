-- file:create_index.sql ln:762 expect:true
CREATE INDEX CONCURRENTLY concur_index4 on concur_heap(f2) WHERE f1='a'
