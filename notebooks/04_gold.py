MY_ID = "chirag"
VOL = f"/Volumes/workspace/capstone_{MY_ID}/raw"

import pyspark.sql.functions as F
from pyspark.sql import Window

sc = spark.table(f"silver_calls_{MY_ID}")
tw = spark.table(f"silver_towers_{MY_ID}")

sc = (sc.withColumn("call_date", F.to_date("start_ts_cast"))
        .withColumn("hour_of_day", F.hour("start_ts_cast")))

agg = (sc.groupBy("tower_id","call_date","hour_of_day")
    .agg(F.count("*").alias("calls"),
         F.sum(F.col("is_dropped").cast("int")).alias("dropped_calls"),
         F.avg("duration_int").alias("avg_duration_seconds")))

starts = sc.select("tower_id","call_date","hour_of_day", F.col("start_ts_cast").alias("t"), F.lit(1).alias("delta"))
ends   = sc.select("tower_id","call_date","hour_of_day", F.col("end_ts").alias("t"), F.lit(-1).alias("delta"))
events = starts.unionByName(ends)

w = Window.partitionBy("tower_id","call_date","hour_of_day").orderBy("t", F.desc("delta"))
running = events.withColumn("running", F.sum("delta").over(w))
peak = running.groupBy("tower_id","call_date","hour_of_day").agg(F.max("running").alias("peak_concurrent"))

gold = (agg.join(peak, ["tower_id","call_date","hour_of_day"])
    .join(tw.select("tower_id","site_name","city","district",
                     F.col("capacity_channels").cast("int").alias("capacity_channels")), "tower_id")
    .withColumn("drop_rate", F.col("dropped_calls")/F.col("calls"))
    .withColumn("utilisation", F.col("peak_concurrent")/F.col("capacity_channels")))

print(gold.count())  # expect 43142
gold.write.mode("overwrite").saveAsTable(f"gold_tower_hour_{MY_ID}")