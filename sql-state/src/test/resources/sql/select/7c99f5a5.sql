-- file:tstypes.sql ln:116 expect:true
SELECT to_tsvector('simple', '1 2 11 3') @@ '1 <-> 3' AS "false"
