-- file:arrays.sql ln:63 expect:true
SELECT *
   FROM arrtest
   WHERE a[1] < 5 and
         c = '{"foobar"}'::_name
