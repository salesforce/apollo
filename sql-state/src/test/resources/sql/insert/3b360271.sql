-- file:numeric_big.sql ln:617 expect:true
INSERT INTO num_result SELECT id, 0, LN(ABS(val))
    FROM num_data
    WHERE val != '0.0'
