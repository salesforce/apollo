-- file:object_address.sql ln:124 expect:true
SELECT pg_get_object_address('server', '{one}', '{}')
