-- file:rowtypes.sql ln:79 expect:true
select ROW(1,2) < ROW(1,3) as true
