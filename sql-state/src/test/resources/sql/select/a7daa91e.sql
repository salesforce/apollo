-- file:stats_ext.sql ln:31 expect:true
SELECT pg_get_statisticsobjdef(oid) FROM pg_statistic_ext WHERE stxname = 'ab1_a_b_stats'
