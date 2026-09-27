MY_ID = "chirag"
VOL = f"/Volumes/workspace/capstone_{MY_ID}/raw"

import pyspark.sql.functions as F
from pyspark.sql import Window

calls  = spark.table(f"bronze_calls_{MY_ID}")
towers = spark.table(f"bronze_towers_{MY_ID}")

c = (calls
     .withColumn("duration_int", F.expr("try_cast(duration_seconds as int)"))
     .withColumn("start_ts_cast", F.expr("try_cast(start_ts as timestamp)")))

w = Window.partitionBy("call_id").orderBy("_ingested_at")
c = c.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
print("after dedupe:", c.count())  # expect 374400

rej_dur   = c.filter("duration_seconds = 'NA'").withColumn("reason", F.lit("duration_not_numeric"))
rej_tower = c.filter("tower_id = 'T999'").withColumn("reason", F.lit("unknown_tower"))
rej_range = c.filter("duration_int is not null and (duration_int < 0 or duration_int > 7200)") \
             .withColumn("reason", F.lit("duration_out_of_range"))

silver_rejects = rej_dur.unionByName(rej_tower, allowMissingColumns=True) \
                         .unionByName(rej_range, allowMissingColumns=True)

silver_calls = (c
    .filter("duration_seconds != 'NA'")
    .filter("tower_id != 'T999'")
    .filter("duration_int is null or (duration_int >= 0 and duration_int <= 7200)")
    .withColumn("end_cause_clean", F.upper(F.trim("end_cause")))
    .withColumn("is_dropped", F.col("end_cause_clean") == "DROPPED")
    .withColumn("end_ts", F.expr("timestampadd(second, duration_int, start_ts_cast)"))
)
print("silver count:", silver_calls.count())  # expect 372150

silver_calls.write.mode("overwrite").saveAsTable(f"silver_calls_{MY_ID}")
silver_rejects.write.mode("overwrite").saveAsTable(f"silver_rejects_{MY_ID}")
towers.write.mode("overwrite").saveAsTable(f"silver_towers_{MY_ID}")