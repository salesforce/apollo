-- file:tstypes.sql ln:122 expect:true
SELECT to_tsvector('simple', '1 2 3 4') @@ '1 <2> (2 <-> 3)' AS "false"
