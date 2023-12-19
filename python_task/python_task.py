# %%
import pandas as pd
import psycopg2
from datetime import date, timedelta
from sqlalchemy import create_engine


def get_count_weekday_df():
    """
    To get the weekday only from started date until the last day of current month,
    Hence, it can be joined when we meet the edges (join_date, resign_date)
    In this pipeline we choose the end date equal than today,
    it reusable for the next ingestion.

    Args:
    None

    Returns:
    dataframe : weekday_date_df
    """

    start_date = '2018-01-01' # Helper Initial date
    # get last day of the current month
    input_dt = date.today()
    next_month = input_dt.replace(day=28) + timedelta(days=4)
    end_date = next_month - timedelta(days=next_month.day)

    date_series = pd.date_range(start=start_date, end=end_date, freq='D')

    # We exclude saturday and sunday
    weekday_dates = date_series[~date_series.dayofweek.isin([5, 6])]

    # Creating a DataFrame with the filtered dates
    date_df = pd.DataFrame({'weekday_date': weekday_dates})

    # extract year and month from date column
    date_df['year-month'] = pd.to_datetime(date_df['weekday_date'], format='%Y-%m-%d').dt.strftime('%Y-%m')

    # group weekday date by year and month
    temp_table = (date_df
          .groupby(['year-month'])
          .agg({'weekday_date': 'count'})
          .reset_index()
          .rename(columns={'weekday_date': 'total_weekday'}))
    
    return temp_table



def extract(csv_path):

    # read csv using pandas
    data = pd.read_csv(csv_path)

    return data


def transorm(timesheets_data, employees_data):
    """
    To transform data based on requirment of destination table

    Args:
    dataframe: timesheets_data.
    dataframe: employees_data.

    Returns:
    dataframe: transformed_data
    """
    # drop rows which has null values
    timesheets_data.dropna(inplace=True)

    # extract year and month from date column
    timesheets_data['year-month'] = pd.to_datetime(timesheets_data['date'], format='%Y-%m-%d').dt.strftime('%Y-%m')

    # Convert time string to time delta to make it easier for perfoming substraction
    timesheets_data['checkout'] = pd.to_timedelta(timesheets_data['checkout'])
    timesheets_data['checkin'] = pd.to_timedelta(timesheets_data['checkin'])
    timesheets_data['work_hours'] = pd.to_timedelta(timesheets_data['checkout']) - pd.to_timedelta(timesheets_data['checkin'])

    # create temporary table to aggregate and group timesheets by employee_id, year, month
    temp_table = (timesheets_data
            .groupby(['employee_id', 'year-month'])
            .agg({'work_hours': 'sum', 'date': 'count'})
            .reset_index()
            .rename(columns={'work_hours':'total_hours', 'date':'days_attended'}))

    # get weekday table
    weekday_group_date = get_count_weekday_df()

    # Join the table with employees_data & weekday_group_date
    temp_table2 = (temp_table
                .merge(employees_data, how='left', on='employee_id')
                .merge(weekday_group_date, how='left', on=['year-month'])
                    )

    # helper year-month for join & resign dates
    temp_table2['join_year-month'] = pd.to_datetime(temp_table2['join_date'], format='%Y-%m-%d').dt.strftime('%Y-%m')
    temp_table2['resign_year-month'] = pd.to_datetime(temp_table2['resign_date'], format='%Y-%m-%d').dt.strftime('%Y-%m')

    # Handling for edges condition join & resign date with proportional days attended in that month 
    temp_table2.loc[
        (temp_table2['join_year-month'] == temp_table2['year-month']) | 
        (temp_table2['resign_year-month'] == temp_table2['year-month'])
                    , 'new_salary'] = round((temp_table2['days_attended']/temp_table2['total_weekday'])*temp_table2['salary'],2)

    # Final transformation
    final_transformed_data = (temp_table2
                            .groupby(['branch_id', 'year-month'])
                            .agg({'employee_id': 'nunique', 'salary':'sum', 'total_hours': 'sum'})
                            .reset_index()
                            .rename(columns={'salary':'total_salary', 'employee_id':'total_employees'}))
    #  Obtain salary per hour
    final_transformed_data['salary_per_hour'] = round(final_transformed_data['total_salary']/(final_transformed_data['total_hours'].dt.total_seconds()/3600),2)

    return final_transformed_data


def load(transformed_data):
    
    # Declare connection to the database
    conn_string = 'postgresql://username:password@host:port/database_name'
    #perform to_sql test and print result
    db = create_engine(conn_string)
    conn = db.connect()
    # Performing incremental load by setting 'append' if table is exist
    transformed_data.to_sql('destination_table', con=conn, if_exists='append', index=False)


def automated_pipeline(timesheets_path, employees_path):
    # Extract Phase
    timesheets_data = extract(timesheets_path)
    employees_data = extract(employees_path)
    # Transform data
    transformed_data = transorm(timesheets_data, employees_data)
    # Load to destination table
    load(transformed_data)



# Test to run the pipeline

if __name__ == "__main__":
    # define timesheets and employees csv files path
    timesheets_path = "timesheets.csv"
    employees_path = "employees.csv"

    # Run ETL pipeline function
    automated_pipeline(timesheets_path, employees_path)



