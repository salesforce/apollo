-- file:tstypes.sql ln:100 expect:true
SELECT 'a b:89  ca:23A,64c cb:80b d:34c'::tsvector @@ 'd:AC & c:*B' as "true"
