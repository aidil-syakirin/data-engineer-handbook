from pyspark.sql import SparkSession





def do_deduped_devices_users(spark, dataframe):
    query = f"""
    with 
        devices_deduped as (
        select 	
                row_number() over (partition by device_id) as row_num,
                device_id,
                browser_type
        from devices
        where 
        device_id is not null	
        ),
        
        devices as(
        select 	
                *
        from devices_deduped
        where row_num = 1
        ),	
        
        today_deduped as (
        select 	
                row_number() over (partition by user_id, device_id) as row_num,
                user_id,
                device_id,
                event_time
        from events
        where 
        date(cast(event_time as timestamp)) = date('2023-01-07')
        and 
        user_id is not null
        and 
        device_id is not null
        ), 

        today as (
        select 	
                user_id,
                device_id,
                date(cast(event_time as timestamp)) as event_time
        from today_deduped
        where row_num = 1
        ),
    
        device_today as (
        select 
            row_number() over (partition by user_id, browser_type ) as row_num,
            t.user_id,
            d.browser_type,
            t.event_time
        from today t 
        left join devices d 
        on t.device_id = d.device_id	
        )
        
        select 
            user_id,
            browser_type,
            event_time
        from device_today
        where row_num = 1	
        """
    dataframe[0].createOrReplaceTempView("devices")
    dataframe[1].createOrReplaceTempView("events")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("game_details") \
      .getOrCreate()
    output_df = do_deduped_devices(spark, spark.table("devices"))
    output_df.write.mode("overwrite").insertInto("devices_agg")