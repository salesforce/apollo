-- file:rangetypes.sql ln:93 expect:true
select numrange(1.1, 2.2) - numrange(2.0, 3.0)
