-- file:arrays.sql ln:700 expect:true
SELECT width_bucket('5'::text, ARRAY[3, 4]::integer[])
