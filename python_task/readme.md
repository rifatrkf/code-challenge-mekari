# Python Task

This is python version to answer the same task as `sql_task`, the difference is the mode of load method that is Incremental Load to the destionation table.
The algorithm (problem & assumptions) is similiar to the previous task.

### Library used
- Pandas -> suitable to perform data transformation for small data
- SQL Alchemy -> Create Connection to the database 
- datetime -> perform date conversion

### Functions
The script concist of 5 functions

1. get_count_weekday_df: To get data frame group of total week day in month year
2. Extract : To read csv files as dataframe
3. Transform : Doing transformation of data based on requirment given
4. Load : To load the transformed data into the database
5. automate pipeline : execute all of the ETL function to make it easier to reuse/import

   
