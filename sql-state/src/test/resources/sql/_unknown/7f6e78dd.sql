-- file:union.sql ln:165 expect:true
(((SELECT q1 FROM int8_tbl INTERSECT SELECT q2 FROM int8_tbl ORDER BY 1))) UNION ALL SELECT q2 FROM int8_tbl
