-- file:foreign_data.sql ln:382 expect:true
ALTER FOREIGN TABLE IF EXISTS doesnt_exist_ft1 RENAME TO foreign_table_1
