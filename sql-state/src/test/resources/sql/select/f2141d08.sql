-- file:tstypes.sql ln:176 expect:true
SELECT ts_rank_cd(' a:1 s:2 d:2A g'::tsvector, 'a <-> s')
