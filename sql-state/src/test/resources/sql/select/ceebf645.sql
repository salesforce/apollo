-- file:rangetypes.sql ln:114 expect:true
select range_merge(numrange(1.0, 2.0), numrange(1.5, 3.0))
