-- file:select_implicit.sql ln:74 expect:true
SELECT a/2, a/2 FROM test_missing_target
	GROUP BY a/2 ORDER BY a/2
