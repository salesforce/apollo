-- file:rangetypes.sql ln:115 expect:true
select range_merge(numrange(1.0, 2.0), numrange(2.5, 3.0))
