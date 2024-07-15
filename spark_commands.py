# Load from csv to dataframe

df = spark.read.format("csv") \
    .option("inferSchema", "false") \
    .option("header", "false") \
    .option("sep", ",") \
    .load("/FileStore/trips.csv")

display(df)


# Load from dataframe to datalake

df.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/trips")
