-- file:create_table.sql ln:556 expect:true
CREATE TABLE fail_part PARTITION OF range_parted3 FOR VALUES FROM (0, minvalue) TO (0, 1)
