-- file:event_trigger.sql ln:272 expect:true
CREATE TABLE evttrig.part_10_20 PARTITION OF evttrig.parted (id)
  FOR VALUES FROM (10) TO (20) PARTITION BY RANGE (id)
