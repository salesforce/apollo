-- file:tstypes.sql ln:93 expect:true
SELECT 'a b:89  ca:23A,64b d:34c'::tsvector @@ 'd:AC & ca:A' as "true"
