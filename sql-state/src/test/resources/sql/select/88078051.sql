-- file:numeric.sql ln:696 expect:true
SELECT width_bucket(5.0::float8, 3.0::float8, 4.0::float8, -5)
