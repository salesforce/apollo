-- file:create_view.sql ln:141 expect:true
SELECT relname FROM pg_class
    WHERE relname LIKE 'v%'
    AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_temp%')
    ORDER BY relname
