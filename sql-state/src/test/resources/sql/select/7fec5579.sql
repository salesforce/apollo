-- file:tstypes.sql ln:157 expect:true
SELECT ts_rank(' a:1 s:2 d g'::tsvector, 'a | s')
