-- file:arrays.sql ln:143 expect:true
UPDATE arrtest_s SET a[3:] = '{23, 24, 25}', b[2:][2:] = '{{25,26}, {28,29}}'
