-- Create destination table if it doesn't exist
CREATE TABLE IF NOT EXISTS destination_table (
    year INTEGER,
    month INTEGER,
	branch_id INTEGER,
	total_employees INTEGER,
	total_salary BIGINT,
	total_hours INTERVAL,
	salary_per_hour numeric
);


-- Performing  full-snapshot mode

-- Truncate the table to empty the data inside the table 
TRUNCATE TABLE destination_table;

-- Load Data into destination table
INSERT INTO destination_table
WITH 
	temp_table AS( 
		SELECT 
			t.employee_id,
			SUM(checkout - checkin) AS total_hours,
			CAST(COUNT(date) AS INTEGER) AS days_attended,
			CAST(EXTRACT(YEAR FROM date) AS INTEGER) AS year,
			CAST(EXTRACT(MONTH FROM date) AS INTEGER) AS month
		FROM timesheets AS t
		WHERE checkin IS NOT NULL AND checkout IS NOT NULL
		GROUP BY t.employee_id, year, month
	),
	
	count_weekday AS(
		SELECT
			EXTRACT(YEAR FROM weekday_date) AS year,
			EXTRACT(MONTH FROM weekday_date) AS month,
			CAST(COUNT(weekday_date) AS INTEGER) AS total_weekday
		FROM(SELECT CAST(dd AS date) AS weekday_date
				FROM generate_series(
					CAST('2018-01-01' AS timestamp),
					CAST('2023-12-31' AS timestamp),
					CAST('1 day' AS interval)
				) dd
				WHERE EXTRACT(ISODOW FROM dd) NOT IN (6,7)
			) AS weekdays_tb
		GROUP BY year, month
	),
	
	temp_table2 AS(
		SELECT
			e.branch_id,
			tmp.employee_id,
			total_hours,
			days_attended,
			cw.total_weekday,
			tmp.year,
			tmp.month,
			join_date,
			resign_date,
			ROUND(CAST( tmp.days_attended AS numeric)/cw.total_weekday ,2),
			CASE
				WHEN 
					(EXTRACT (YEAR FROM join_date)=tmp.year AND EXTRACT(MONTH FROM join_date)=tmp.month)
					OR
					(EXTRACT (YEAR FROM resign_date)=tmp.year AND EXTRACT(MONTH FROM resign_date)=tmp.month)
					THEN ROUND(CAST( tmp.days_attended AS numeric)/cw.total_weekday ,2)*salary
					ELSE salary END AS salary
		FROM temp_table AS tmp
		LEFT JOIN employees AS e ON e.employee_id = tmp.employee_id
		LEFT JOIN count_weekday AS cw ON cw.month = tmp.month AND cw.year=tmp.year
	)

SELECT 
	year,
	month,
	branch_id,
	CAST(COUNT(DISTINCT employee_id) AS INTEGER) AS total_employees,
	SUM(salary) as total_salary ,
	SUM(total_hours) AS total_hours,
	ROUND(CAST(SUM(salary)/(EXTRACT(epoch FROM SUM(total_hours))/3600) AS numeric),2) AS salary_per_hour
FROM temp_table2
GROUP BY year,
	month,
	branch_id
ORDER BY year,
	month,
	branch_id;