-- file:create_table.sql ln:627 expect:true
CREATE TABLE range_parted4_1 PARTITION OF range_parted4 FOR VALUES FROM (MINVALUE, MINVALUE, MINVALUE) TO (1, MAXVALUE, MAXVALUE)
