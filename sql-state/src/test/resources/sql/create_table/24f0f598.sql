-- file:create_table.sql ln:561 expect:true
CREATE TABLE fail_part PARTITION OF range_parted3 FOR VALUES FROM (1, 10) TO (1, 20)
