-- file:gist.sql ln:65 expect:true
explain (costs off)
select p from gist_tbl where p <@ box(point(0,0), point(0.5, 0.5))
order by point(0.101, 0.101) <-> p
