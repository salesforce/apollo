-- file:groupingsets.sql ln:63 expect:true
select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by rollup (a,b) order by b desc, a
