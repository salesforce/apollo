-- file:triggers.sql ln:772 expect:true
UPDATE main_view SET b = 32 WHERE a = 21 AND b = 31 RETURNING a, b
