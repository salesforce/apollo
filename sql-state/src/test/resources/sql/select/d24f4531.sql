-- file:arrays.sql ln:56 expect:true
SELECT array_ndims(a) AS a,array_ndims(b) AS b,array_ndims(c) AS c
   FROM arrtest
