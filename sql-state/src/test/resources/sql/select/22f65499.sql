-- file:interval.sql ln:223 expect:true
SELECT interval '1 2:03.5678' minute to second(2)
