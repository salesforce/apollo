-- file:rangetypes.sql ln:102 expect:true
select numrange(1.0, 3.0,'()') << numrange(3.0, 4.0,'()')
