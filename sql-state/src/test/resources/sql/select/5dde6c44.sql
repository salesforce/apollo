-- file:union.sql ln:116 expect:true
(SELECT 1,2,3 UNION SELECT 4,5,6) INTERSECT SELECT 4,5,6
