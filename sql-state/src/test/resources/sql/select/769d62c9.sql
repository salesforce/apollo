-- file:tstypes.sql ln:184 expect:true
SELECT 'a:1 b:2'::tsvector @@ 'a <-> b'::tsquery AS "true"
