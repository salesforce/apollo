-- file:char.sql ln:49 expect:true
SELECT '' AS six, c.*
   FROM CHAR_TBL c
   WHERE c.f1 <= 'a'
