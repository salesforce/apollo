-- file:foreign_data.sql ln:364 expect:true
ALTER FOREIGN TABLE IF EXISTS doesnt_exist_ft1 ADD COLUMN c10 integer OPTIONS (p1 'v1')
