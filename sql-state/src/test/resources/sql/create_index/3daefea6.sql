-- file:create_index.sql ln:751 expect:true
CREATE INDEX CONCURRENTLY IF NOT EXISTS concur_index1 ON concur_heap(f2,f1)
