-- file:rangetypes.sql ln:88 expect:true
select range_adjacent(numrange(2.0, 3.0, '(]'), numrange(1.0, 2.0, '(]'))
