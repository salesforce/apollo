-- file:arrays.sql ln:702 expect:true
SELECT width_bucket(5, ARRAY[ARRAY[1, 2], ARRAY[3, 4]])
