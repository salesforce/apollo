-- file:stats_ext.sql ln:71 expect:true
CREATE TABLE tststats.pt1 PARTITION OF tststats.pt FOR VALUES FROM (-10, -10) TO (10, 10)
