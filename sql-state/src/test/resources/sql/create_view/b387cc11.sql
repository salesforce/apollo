-- file:create_view.sql ln:158 expect:true
CREATE VIEW temporal3 AS SELECT * FROM t1 LEFT JOIN tt ON t1.num = tt.num2
