-- file:rangetypes.sql ln:447 expect:true
select array[1,1] <@ arrayrange(array[1,2], array[2,1])
