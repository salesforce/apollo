-- file:geometry.sql ln:35 expect:true
SELECT '' AS two, p1.f1
   FROM POINT_TBL p1
   WHERE p1.f1 ?- point '(0,0)'
