-- file:tstypes.sql ln:170 expect:true
SELECT ts_rank_cd(' a:1 s:2B d g'::tsvector, 'a & s')
