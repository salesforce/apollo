-- file:collate.linux.utf8.sql ln:75 expect:true
CREATE TABLE collate_test5 (
    a int,
    b testdomain_sv COLLATE "en_US"
)
