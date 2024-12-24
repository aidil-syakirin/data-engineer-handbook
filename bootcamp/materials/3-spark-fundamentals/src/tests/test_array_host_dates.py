from chispa.dataframe_comparer import *

from ..jobs.array_host_dates_job import do_array_host_dates
from collections import namedtuple
from datetime import date

today = namedtuple("Today", "host event_time")
yesterday = namedtuple("Yesterday", "host dates_active date")
hostcumulated = namedtuple("host_cumulated", "host dates_active date")


def test_array_host_dates_generation(spark):
    source_today = [
        today('admin.zachwilson.tech', '2023-01-03 05:37:30.606000'),
        today('admin.zachwilson.tech', '2023-01-03 07:40:28.606000'),
        today('www.zachwilson.tech', '2023-01-03 10:37:23.606000'),
        today('www.zachwilson.tech', '2023-01-03 12:17:28.606000')
    ]
    source_yesterday = [
        yesterday('admin.zachwilson.tech', [date(2023,1,1)], date(2023,1,1)),
        yesterday('www.eczachly.com', [date(2023,1,1)], date(2023,1,1)),
        yesterday('admin.zachwilson.tech', [date(2023,1,2), date(2023,1,1)], date(2023,1,2)),
        yesterday('www.eczachly.com', [date(2023,1,2), date(2023,1,1)], date(2023,1,2))
    ]
    
    df_today = spark.createDataFrame(source_today)
    df_yesterday = spark.createDataFrame(source_yesterday)

    actual_df = do_array_host_dates(spark, df_today, df_yesterday)
    expected_data = [
        hostcumulated('admin.zachwilson.tech',[date(2023,1,3), date(2023,1,2), date(2023,1,1)], date=date(2023,1,3)),
        hostcumulated('www.eczachly.com', [date(2023,1,2), date(2023,1,1)], date=date(2023,1,3)),
        hostcumulated('www.zachwilson.tech', [date(2023,1,3)], date=date(2023,1,3))
   ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)