-- file:aggregates.sql ln:40 expect:true
SELECT stddev_pop(3.0::numeric), stddev_samp(4.0::numeric)
