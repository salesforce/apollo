-- file:rangetypes.sql ln:103 expect:true
select numrange(1.0, 2.0) >> numrange(3.0, 4.0)
