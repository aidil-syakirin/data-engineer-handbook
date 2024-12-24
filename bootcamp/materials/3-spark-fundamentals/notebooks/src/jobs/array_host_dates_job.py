from pyspark.sql import SparkSession





def do_array_host_dates(spark, df_today, df_yesterday):
    query = f"""
  with 
      today_o as (
	select
		host,
		date(cast(event_time as timestamp)) as event_time
	from today
    where date(cast(event_time as timestamp)) = date('2023-01-03')
	group by host, date(cast(event_time as timestamp))
),
	yesterday_o as (
	select * 
	from yesterday
	where date = date('2023-01-02')
)	
	select 
	coalesce(t.host, y.host) as host,
	case 
		when y.dates_active is null
		then array(t.event_time)
		when t.event_time is null then y.dates_active
		else concat(array(t.event_time), y.dates_active) 
		end as dates_active, 
	date(coalesce (t.event_time, y.date + interval '1 day')) 
	as date
	from today_o t full outer join yesterday_o y
	on t.host = y.host 
        """
    df_today.createOrReplaceTempView("today")
    df_yesterday.createOrReplaceTempView("yesterday")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("game_details") \
      .getOrCreate()
    output_df = do_deduped_devices(spark, spark.table("devices"))
    output_df.write.mode("overwrite").insertInto("devices_agg")