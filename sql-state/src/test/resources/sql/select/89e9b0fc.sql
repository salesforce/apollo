-- file:interval.sql ln:151 expect:true
SELECT justify_interval(interval '1 month -1 hour') as "1 month -1 hour"
