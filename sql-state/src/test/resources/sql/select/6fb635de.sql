-- file:rangetypes.sql ln:77 expect:true
select * from numrange_test where nr > numrange(-1001.0, -1000.0,'[]')
