-- file:join.sql ln:91 expect:true
SELECT '' AS "xxx", *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b
