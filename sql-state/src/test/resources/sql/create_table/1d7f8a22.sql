-- file:create_table.sql ln:448 expect:true
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES FROM (1) TO (2)
