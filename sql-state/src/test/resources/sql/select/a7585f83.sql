-- file:join.sql ln:311 expect:true
select * from x left join y on (x1 = y1 and y2 is not null)
