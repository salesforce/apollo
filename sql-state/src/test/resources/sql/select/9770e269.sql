-- file:create_index.sql ln:925 expect:true
SELECT unique1, unique2 FROM onek_with_null
  ORDER BY unique2 DESC LIMIT 2
