-- file:alter_table.sql ln:318 expect:true
ALTER TABLE tmp3 ADD CONSTRAINT b_le_20 CHECK (b <= 20) NOT VALID
