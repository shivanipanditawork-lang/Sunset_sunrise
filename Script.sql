SELECT * FROM sunrise_data ;

SELECT s.*
FROM sunrise_data s
JOIN (
    SELECT strftime('%Y', date) AS year,
           MAX(day_length) AS max_day_length
    FROM sunrise_data
    GROUP BY year
) grouped
ON strftime('%Y', s.date) = grouped.year
AND s.day_length = grouped.max_day_length
ORDER BY year;

SELECT s.*
FROM sunrise_data s
JOIN (
    SELECT strftime('%Y', date) AS year,
           MIN(day_length) AS min_day_length
    FROM sunrise_data
    GROUP BY year
) grouped
ON strftime('%Y', s.date) = grouped.year
AND s.day_length = grouped.min_day_length
ORDER BY year;

SELECT s.*
FROM sunrise_data s
JOIN (
    SELECT strftime('%Y', date) AS year,
           strftime('%m', date) AS month,
           MAX(sunrise) AS latest_sunrise
    FROM sunrise_data
    GROUP BY year, month
) grouped
ON strftime('%Y', s.date) = grouped.year
AND strftime('%m', s.date) = grouped.month
AND s.sunrise = grouped.latest_sunrise
ORDER BY year, month;