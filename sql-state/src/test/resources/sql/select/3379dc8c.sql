-- file:int4.sql ln:52 expect:true
SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= int2 '0'
