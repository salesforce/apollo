-- file:numerology.sql ln:45 expect:true
INSERT INTO TEMP_INT2 (f1)
  SELECT int2(f1) FROM INT4_TBL
  WHERE (f1 >= -32767) AND (f1 <= 32767)
