-- file:rangetypes.sql ln:72 expect:true
select * from numrange_test where nr < numrange(0.0, 1.0,'[]')
