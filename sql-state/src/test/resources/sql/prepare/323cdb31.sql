-- file:prepare.sql ln:66 expect:true
PREPARE q6 AS
    SELECT * FROM tenk1 WHERE unique1 = $1 AND stringu1 = $2
