-- file:create_index.sql ln:888 expect:true
CREATE UNIQUE INDEX onek_nulltest ON onek_with_null (unique2 desc nulls last,unique1)
