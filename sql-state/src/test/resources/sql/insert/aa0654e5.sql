-- file:numerology.sql ln:62 expect:true
INSERT INTO TEMP_GROUP
  SELECT 2, i.f1, f.f1
  FROM INT4_TBL i, FLOAT8_TBL f
