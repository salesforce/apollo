-- file:typed_table.sql ln:70 expect:true
CREATE TABLE persons3 OF person_type (
    PRIMARY KEY (id),
    name NOT NULL DEFAULT ''
)
