-- file:box.sql ln:51 expect:true
SELECT '' AS two, b1.*
   FROM BOX_TBL b1
   WHERE b1.f1 &< box '(2.0,2.0,2.5,2.5)'
