-- file:arrays.sql ln:255 expect:true
SELECT array_cat(ARRAY[1,2], ARRAY[3,4]) AS "{1,2,3,4}"
