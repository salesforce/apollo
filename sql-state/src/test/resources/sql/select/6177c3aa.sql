-- file:tstypes.sql ln:97 expect:true
SELECT 'a b:89  ca:23A,64b d:34c'::tsvector @@ 'd:AC & c:*CB' as "true"
