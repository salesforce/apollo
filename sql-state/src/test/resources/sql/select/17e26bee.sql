-- file:numeric.sql ln:746 expect:true
SELECT width_bucket('Infinity'::float8, 1, 10, 10),
       width_bucket('-Infinity'::float8, 1, 10, 10)
