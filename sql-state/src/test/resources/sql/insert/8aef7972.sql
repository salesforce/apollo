-- file:numeric_big.sql ln:574 expect:true
INSERT INTO num_result SELECT t1.id, t2.id, round(t1.val * t2.val, 30)
    FROM num_data t1, num_data t2
