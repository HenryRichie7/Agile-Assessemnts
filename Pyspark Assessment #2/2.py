from pyspark.pandas import *
from pyspark.sql.functions import *
from datetime import date as dd
from datetime import datetime, timedelta
import pandas as pd
import calendar

today = dd.today()
total_days = calendar.monthrange(today.year,today.month)[1]

pandsDF_Add_5 = pd.DataFrame({'Date': pd.date_range(start=today,periods=5)})
pandsDF_Sub_5 = pd.DataFrame({'Date': pd.date_range(end=today,periods=5)})

sparkDF_Add5 = spark.createDataFrame(pandsDF_Add_5)
sparkDF_Sub5 = spark.createDataFrame(pandsDF_Sub_5)

sparkDF_Add5.select(to_date(col("Date")).alias("Date")).show()
sparkDF_Sub5.select(to_date(col("Date")).alias("Date")).show()

#Display start_date and end_date of the current quarter.
def get_quarter_dates(date):
    quarter = int((date.month - 1) / 3) + 1
    start_date = datetime(date.year, quarter * 3 - 2,1)
    end_at = calendar.monthrange(start_date.year,start_date.month)[1]
    end_date = datetime(date.year, quarter * 3,end_at)
    return start_date.strftime("%d/%m/%Y"), end_date.strftime("%d/%m/%Y")

q_start_date = get_quarter_dates(today)[0]
q_end_date = get_quarter_dates(today)[1]

print(f"Quarter Start: {q_start_date} Quarter End: {q_end_date}")


# get start date and end date for the current month.
def get_month_dates(date):
    start_date = datetime(date.year, date.month, 1)
    end_date = datetime(date.year, date.month, 1) + timedelta(days=total_days-1)
    return start_date.strftime("%d/%m/%Y"), end_date.strftime("%d/%m/%Y")


M_start_date = get_month_dates(today)[0]
M_end_date = get_month_dates(today)[1]

print(f"Month Start: {M_start_date} Month End: {M_end_date}")
