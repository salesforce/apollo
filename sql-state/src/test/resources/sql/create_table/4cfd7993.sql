-- file:create_table.sql ln:558 expect:true
CREATE TABLE part10 PARTITION OF range_parted3 FOR VALUES FROM (1, minvalue) TO (1, 1)
