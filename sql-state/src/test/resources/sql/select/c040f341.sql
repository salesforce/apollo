-- file:interval.sql ln:212 expect:true
SELECT interval '12:34.5678' minute to second(2)
