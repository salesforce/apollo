-- file:arrays.sql ln:293 expect:true
SELECT array_positions('[2:4]={1,2,3}'::int[], 1)
