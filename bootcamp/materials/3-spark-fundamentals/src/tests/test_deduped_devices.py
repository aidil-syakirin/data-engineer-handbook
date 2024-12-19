from chispa.dataframe_comparer import *

from ..jobs.deduped_devices_job import do_deduped_devices_users
from collections import namedtuple

Devices = namedtuple("Devices", "device_id browser_type")
Users = namedtuple("User", "user_id device_id event_time")
DevicesUsersToday = namedtuple("devices_users_today", "user_id browser_type event_time")


def test_devices_users_today_generation(spark):
    source_data = [
    [
        Devices(1, 'Googlebot'),
        Devices(2, 'Googlebot'),
        Devices(3, 'Chrome')
        # ,Devices(NULL, 'Chrome')
    ],
    [
       Users(444502572952128450, 1, '2023-01-07'),
        Users(444502572952128450, 2, '2023-01-07'),
        Users(551889145147261600, 3, '2023-01-07') 
    ]
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_deduped_devices_users(spark, source_df)
    expected_data = [
        DevicesUsersToday(444502572952128450, 'Googlebot','2023-01-07'),
        DevicesUsersToday(551889145147261600, 'Chrome', '2023-01-07')
   ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)