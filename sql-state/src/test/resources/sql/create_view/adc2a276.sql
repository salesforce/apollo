-- file:create_view.sql ln:178 expect:true
CREATE   VIEW  pubview AS SELECT * FROM tbl1 WHERE tbl1.a
BETWEEN (SELECT d FROM tbl2 WHERE c = 1) AND (SELECT e FROM tbl3 WHERE f = 2)
AND EXISTS (SELECT g FROM tbl4 LEFT JOIN tbl3 ON tbl4.h = tbl3.f)
