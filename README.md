# code-challenge-mekari



This project is to fulfill Challenge Test in process of recruitment of Data Engineer in Mekari.

- Author: Rifat Rachim Khatami Fasha
- Contact: rifat25khatami@gmail.com

## Project Structure

The project is organized into the following folders:

- `sql_task`: Contains SQL-related tasks. See [sql_task/readme.md](./sql_task/README.md) for details.
- `python_task`: Includes Python-related tasks. See [python_task/readme.md](./python_task/README.md) for details.

## Business Case
There are two tables:

- employees - all-time employee information (CSV)
- timesheets - daily clock-ins and clock-outs of the employees (CSV)

As a Data Engineer, we are working with an analyst to figure out whether the current payroll
scheme (which is a per-month basis) is reasonably cost-effective in terms of cost per hour. It
needs to be analyzed per branch and a monthly basis.

For each branch, find out salary per hour based on the number of employees that work for that
branch each month. For E.g. assuming Branch A has 5 people working for it in January; the
salary for those 5 people in that month is Rp100,000,000, and the total hours for the same 5
employees in that month is 1000 hours. Therefore, the output should be Rp100,000 per hour.
The output should be loaded to a table that will be read by a BI tool with a straightforward SQL
query: SELECT year, month, branch_id, salary_per_hour FROM ….
