-- file:arrays.sql ln:59 expect:true
SELECT array_dims(a) AS a,array_dims(b) AS b,array_dims(c) AS c
   FROM arrtest
