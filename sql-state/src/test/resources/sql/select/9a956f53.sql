-- file:create_view.sql ln:191 expect:true
SELECT count(*) FROM pg_class where relname LIKE 'mytempview'
And relnamespace IN (SELECT OID FROM pg_namespace WHERE nspname LIKE 'pg_temp%')
