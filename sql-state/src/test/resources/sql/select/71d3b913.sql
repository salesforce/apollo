-- file:rowtypes.sql ln:101 expect:true
select ROW(1,2) in (ROW(3,4), ROW(1,2::int8))
