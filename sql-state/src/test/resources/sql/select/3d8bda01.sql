-- file:polygon.sql ln:89 expect:true
SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' <@ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS false
