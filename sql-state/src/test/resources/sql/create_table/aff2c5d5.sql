-- file:alter_table.sql ln:233 expect:true
CREATE TABLE constraint_rename_test2 (a int CONSTRAINT con1 CHECK (a > 0), d int) INHERITS (constraint_rename_test)
