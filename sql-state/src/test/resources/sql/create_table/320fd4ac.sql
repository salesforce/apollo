-- file:enum.sql ln:248 expect:true
CREATE TABLE enumtest_child (parent rainbow REFERENCES enumtest_parent)
