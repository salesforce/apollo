-- file:rangetypes.sql ln:84 expect:true
select range_adjacent(numrange(2.0, 3.0), numrange(3.1, 4.0))
