-- file:triggers.sql ln:267 expect:true
UPDATE main_table SET a = a + 1 WHERE b < 30
