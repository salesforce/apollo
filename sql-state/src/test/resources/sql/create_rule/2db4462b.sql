-- file:copydml.sql ln:46 expect:true
create rule qqq as on update to copydml_test do also delete from copydml_test
