SELECT (((open - close)/ close) * 100) AS pct_change
FROM spy_sec_eod_aggs
WHERE
    DAY(date_time) = 3
    AND HOUR(date_time) = 9
    AND MINUTE(date_time) = 30;