-- file:tstypes.sql ln:156 expect:true
SELECT ts_rank(' a:1 s:2B d g'::tsvector, 'a | s')
