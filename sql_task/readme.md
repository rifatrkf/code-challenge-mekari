# SQL Task

## Introduction
As it has been mentioned in the first page, We will create an SQL script that performs a data transformation process by extracting information from two tables, namely 'employees' and 'timesheets.' This script will merge, reformat, and load the transformed data into a destination table. This is expected to run daily in full-snapshot mode.

## Query Explanation
As we can see in the query, it was divided into numbers of command

### Create Table If Not Exist
This is to to ensure that the table is not recreated if it already exists in the database.

### Truncate the table
Due to the query running in full load mode, Truncate is used to empty the table before inserting the new one. Hence, it will prevent us to have duplicated data after loading process to destination table

### Extract and Transformation
The next step is the main process of this task, on how we transform the data
