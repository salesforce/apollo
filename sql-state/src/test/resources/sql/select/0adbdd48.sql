-- file:bit.sql ln:167 expect:true
SELECT POSITION(B'1101' IN b),
       POSITION(B'11011' IN b),
       b
       FROM BIT_SHIFT_TABLE 
