-- file:numeric_big.sql ln:629 expect:true
INSERT INTO num_result SELECT id, 0, LOG('10'::numeric, ABS(val))
    FROM num_data
    WHERE val != '0.0'
