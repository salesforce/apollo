-- file:bit.sql ln:76 expect:true
SELECT a,a<<4 AS "a<<4",b,b>>2 AS "b>>2" FROM varbit_table
