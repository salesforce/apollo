-- file:rangetypes.sql ln:64 expect:true
SELECT * FROM numrange_test WHERE range_contained_by(numrange(-1e7,-10000.1), nr)
