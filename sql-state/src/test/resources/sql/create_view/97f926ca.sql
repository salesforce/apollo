-- file:create_view.sql ln:128 expect:true
CREATE VIEW v11_temp AS SELECT t1.id, t2.a FROM base_table t1, v10_temp t2
