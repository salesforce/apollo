-- file:foreign_key.sql ln:785 expect:true
ALTER TABLE fktable ADD CONSTRAINT fk_123_123
FOREIGN KEY (x1,x2,x3) REFERENCES pktable(id1,id2,id3)
