-- file:groupingsets.sql ln:97 expect:true
select sum(c) from gstest2
  group by grouping sets(a, grouping sets(a, cube(b)))
  order by 1 desc
