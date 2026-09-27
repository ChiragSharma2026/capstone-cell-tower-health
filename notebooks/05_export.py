MY_ID = "chirag"
VOL = f"/Volumes/workspace/capstone_{MY_ID}/raw"
GOLD_VOL = f"/Volumes/workspace/capstone_{MY_ID}/gold"


gold = spark.table(f"gold_tower_hour_{MY_ID}")
gold.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{GOLD_VOL}/tower_hour_export")


import os
files = dbutils.fs.ls(f"{GOLD_VOL}/tower_hour_export")
for f in files:
    print(f.path, f.size)