-- file:select.sql ln:22 expect:true
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY stringu1 using <
