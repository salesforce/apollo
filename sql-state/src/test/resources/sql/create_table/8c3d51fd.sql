-- file:foreign_key.sql ln:265 expect:true
CREATE TABLE FKTABLE ( ftest1 int, ftest2 int, ftest3 int, ftest4 int,  CONSTRAINT constrname3
			FOREIGN KEY(ftest1, ftest2, ftest3) REFERENCES PKTABLE
			ON DELETE CASCADE ON UPDATE CASCADE)
