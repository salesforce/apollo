-- file:rangetypes.sql ln:106 expect:true
select numrange(1.1, 2.2) < numrange(1.0, 200.2)
