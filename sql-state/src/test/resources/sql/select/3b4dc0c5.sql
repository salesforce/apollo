-- file:privileges.sql ln:614 expect:true
select has_table_privilege(current_user,'pg_authid','insert')
