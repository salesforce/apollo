-- file:groupingsets.sql ln:201 expect:true
select a, b, c, d from gstest2 group by rollup(a,b),grouping sets(c,d)
