-- file:groupingsets.sql ln:94 expect:true
select sum(c) from gstest2
  group by grouping sets(grouping sets(rollup(c), grouping sets(cube(c))))
  order by 1 desc
