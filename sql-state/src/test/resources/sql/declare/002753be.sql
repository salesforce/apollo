-- file:portals.sql ln:27 expect:true
DECLARE foo11 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2
