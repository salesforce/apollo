-- file:tstypes.sql ln:173 expect:true
SELECT ts_rank_cd(' a:1 s:2A d g'::tsvector, 'a <-> s')
