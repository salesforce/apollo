-- file:tstypes.sql ln:140 expect:true
select to_tsvector('simple', 'y y q') @@ '(x | y <-> z) <-> q' AS "false"
