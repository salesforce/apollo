-- file:tsearch.sql ln:431 expect:true
SELECT COUNT(*) FROM test_tsquery WHERE keyword = 'new & york'
