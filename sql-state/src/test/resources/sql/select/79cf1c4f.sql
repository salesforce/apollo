-- file:int4.sql ln:67 expect:true
SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i
