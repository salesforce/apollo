-- file:tstypes.sql ln:155 expect:true
SELECT ts_rank(' a:1 sa:2C d g'::tsvector, 'a | sa:*')
