-- file:window.sql ln:71 expect:true
SELECT lead(ten * 2, 1, -1) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10
