-- file:foreign_data.sql ln:422 expect:true
SELECT has_server_privilege('regress_test_role',
    (SELECT oid FROM pg_foreign_server WHERE srvname='s8'), 'USAGE')
