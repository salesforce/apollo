-- file:vacuum.sql ln:70 expect:true
CREATE TABLE vacparted1 PARTITION OF vacparted FOR VALUES IN (1)
