-- file:numerology.sql ln:17 expect:true
INSERT INTO TEMP_FLOAT (f1)
  SELECT float8(f1) FROM INT2_TBL
