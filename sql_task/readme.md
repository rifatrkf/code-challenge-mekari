# SQL Task

## Introduction
As it has been mentioned in the first page, We will create an SQL script that performs a data transformation process by extracting information from two tables, namely 'employees' and 'timesheets.' This script will merge, reformat, and load the transformed data into a destination table. This is expected to run daily in full-snapshot mode.

## Problems & Assumptions
In order to measure the salary system, let us make some assumptions:
1. salary is measured from start date of month until the last date of month
2. Because the data is half-dummy, we ignore the number of the day of employee working in a month, so the salary is still counted fully.
3. In the Employees Table, it has column `join_date` and  `resign_date`, we know that this case is touching on when we calculate the monthly salary, then we make an exception in the month of employee join and resign, the salary is calculated proportionally based on the number of the day working in that month
4. In order to calculated proportional salary, we calculated weekdays as working days (Holidays are ignored)
5. the data that has missing Clock in or clock out are excluded

## Query Explanation
As we can see in the query, it was divided into numbers of command

### Create Table If Not Exist
This is to to ensure that the table is not recreated if it already exists in the database.

### Truncate the table
Due to the query running in full load mode, Truncate is used to empty the table before inserting the new one. Hence, it will prevent us to have duplicated data after loading process to destination table

### Extract and Transformation
The next step is the main process of this task, on how we transform the data. We utilized the CTE to make it the query process is done easily. There are few of temporary tables that have been made.

1. `temp_table`, is to get the table for grouping the sum of total hours & count days_attended by employee_id, month, year
2. `count_weekday`, is generated for getting total weekday in every month-year, the funtion of this table is when we encounter join & resign date. later, we can calculate the proportional salary.
3. `temp_table2`, is when `temp_table` and `count_weekday` are merged and also the proportional salary is calculated here. Then we join with emplyee table to get the branch id. the temp_table2, is still group by employee_id, year, month

Finally, in the last query we perform aggregations for total working hours, total salary, salary_per_hour by branch id, year and month.
Then we insert the table into the destination table.
    

