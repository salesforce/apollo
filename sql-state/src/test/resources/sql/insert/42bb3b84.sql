-- file:numerology.sql ln:27 expect:true
INSERT INTO TEMP_INT4 (f1)
  SELECT int4(f1) FROM FLOAT8_TBL
  WHERE (f1 > -2147483647) AND (f1 < 2147483647)
