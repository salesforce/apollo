-- file:float8.sql ln:131 expect:true
SELECT '' AS bad, f.f1 ^ '1e200' from FLOAT8_TBL f
