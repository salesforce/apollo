-- file:window.sql ln:188 expect:true
SELECT sum(unique1) over (rows between 2 preceding and 2 following),
	unique1, four
FROM tenk1 WHERE unique1 < 10
