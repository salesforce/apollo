-- file:create_table.sql ln:629 expect:true
CREATE TABLE range_parted4_2 PARTITION OF range_parted4 FOR VALUES FROM (3, 4, 5) TO (6, 7, MAXVALUE)
