-- file:portals.sql ln:43 expect:true
DECLARE foo19 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2
