-- file:rangetypes.sql ln:113 expect:true
select range_merge(numrange(1.0, 2.0), numrange(2.0, 3.0))
