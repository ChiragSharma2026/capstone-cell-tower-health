MY_ID = "chirag"
VOL = f"/Volumes/workspace/capstone_{MY_ID}/raw"

import pyspark.sql.functions as F

def to_bronze(path, name):
    df = spark.read.option("header", True).csv(path)
    cols = df.columns
    df = (df.withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_row_hash", F.sha2(F.concat_ws("||", *cols), 256)))
    df.write.mode("overwrite").saveAsTable(f"bronze_towers_{MY_ID}" if name=="towers" else f"bronze_calls_{MY_ID}")
    return df

bronze_towers = to_bronze(f"{VOL}/raw/towers", "towers")
bronze_calls  = to_bronze(f"{VOL}/raw/calls", "calls")

print(bronze_towers.count(), bronze_calls.count())