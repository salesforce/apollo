-- file:foreign_data.sql ln:341 expect:true
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c9_check CHECK (c9 < 0) NOT VALID
