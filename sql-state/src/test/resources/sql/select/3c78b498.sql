-- file:arrays.sql ln:572 expect:true
select array_agg(ten) from (select ten from tenk1 where unique1 < 15 order by unique1) ss
