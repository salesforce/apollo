-- file:create_table.sql ln:566 expect:true
CREATE TABLE fail_part PARTITION OF range_parted3 FOR VALUES FROM (1, minvalue) TO (1, maxvalue)
