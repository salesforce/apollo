-- file:create_table.sql ln:605 expect:true
CREATE TABLE part_c_1_10 PARTITION OF part_c FOR VALUES FROM (1) TO (10)
