-- file:tsrf.sql ln:92 expect:true
explain (verbose, costs off)
select 'foo' as f, generate_series(1,2) as g from few order by 1
