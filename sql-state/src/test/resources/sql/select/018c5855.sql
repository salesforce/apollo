-- file:tstypes.sql ln:197 expect:true
SELECT strip('w:12B w:13* w:12,5,6 a:1,3* a:3 w asd:1dc asd'::tsvector)
