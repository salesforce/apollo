-- file:numeric_big.sql ln:566 expect:true
INSERT INTO num_result SELECT t1.id, t2.id, t1.val * t2.val
    FROM num_data t1, num_data t2
