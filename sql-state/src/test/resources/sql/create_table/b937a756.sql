-- file:create_table.sql ln:543 expect:true
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (9) TO (maxvalue)
