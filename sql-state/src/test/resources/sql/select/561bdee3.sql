-- file:rowtypes.sql ln:88 expect:true
select ROW(1,2,3) < ROW(1,NULL,4) as null
