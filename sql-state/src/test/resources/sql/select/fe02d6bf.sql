-- file:groupingsets.sql ln:230 expect:true
select a,count(*) from gstest2 group by rollup(a) having a is distinct from 1 order by a
