-- file:window.sql ln:265 expect:true
SELECT nth_value(four, 0) OVER (ORDER BY ten), ten, four FROM tenk1
