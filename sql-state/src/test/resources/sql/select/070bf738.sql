-- file:arrays.sql ln:313 expect:true
SELECT 0 || ARRAY[1,2] || 3 AS "{0,1,2,3}"
