-- file:aggregates.sql ln:386 expect:true
select array_agg(distinct a order by a desc)
  from (values (1),(2),(1),(3),(null),(2)) v(a)
