import time

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

object_schema = StructType(
    [
        StructField("@context", StringType(), True),
        StructField("@type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("image", StringType(), True),
        StructField(
            "itemListElement",
            ArrayType(
                StructType(
                    [
                        StructField("@type", StringType(), True),
                        StructField(
                            "item",
                            StructType(
                                [
                                    StructField("@id", StringType(), True),
                                    StructField("name", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("position", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("location", StringType(), True),
        StructField("logo", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "object",
            StructType(
                [
                    StructField("@type", StringType(), True),
                    StructField(
                        "address",
                        StructType(
                            [
                                StructField("@type", StringType(), True),
                                StructField(
                                    "addressCountry",
                                    StructType(
                                        [
                                            StructField("@type", StringType(), True),
                                            StructField("name", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                                StructField("addressLocality", StringType(), True),
                                StructField("addressRegion", StringType(), True),
                                StructField("postalCode", StringType(), True),
                                StructField("streetAddress", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("description", StringType(), True),
                    StructField(
                        "floorSize",
                        StructType(
                            [
                                StructField("@type", StringType(), True),
                                StructField("unitCode", StringType(), True),
                                StructField("unitText", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "geo",
                        StructType(
                            [
                                StructField("@type", StringType(), True),
                                StructField("latitude", StringType(), True),
                                StructField("longitude", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("name", StringType(), True),
                    StructField("numberOfBathroomsTotal", StringType(), True),
                    StructField("numberOfBedrooms", StringType(), True),
                    StructField("numberOfRooms", StringType(), True),
                    StructField("tourBookingPage", StringType(), True),
                    StructField("url", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "offers",
            StructType(
                [
                    StructField("@type", StringType(), True),
                    StructField("highPrice", StringType(), True),
                    StructField("lowPrice", StringType(), True),
                    StructField("offerCount", StringType(), True),
                    StructField("priceCurrency", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "participant",
            StructType(
                [
                    StructField("@type", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("url", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("sameAs", ArrayType(StringType(), True), True),
        StructField("url", StringType(), True),
    ]
)


def preprocess_columns(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.select(
            "name",
            "description",
            "numberOfRooms",
            "numberOfBathroomsTotal",
            "numberOfBedrooms",
            "tourBookingPage",
            "url",
            F.col("floorSize.unitText").alias("floorSize"),
            "address.addressLocality",
            "address.addressRegion",
            "address.postalCode",
            "address.streetAddress",
            "geo.latitude",
            "geo.longitude",
        )
        .withColumn("region", F.split(F.input_file_name(), r"_"))
        .withColumn("region", F.col("region")[F.size("region") - 2])
    )


def extract_price_from_url(dataframe: DataFrame) -> DataFrame:
    return dataframe.withColumn("price", F.regexp_extract(F.col("url"), r"RS(\d+)", 1))


def remove_bad_data(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.filter(F.col("price") != "")
        .dropna(subset="price")
        .filter(~F.col("floorSize").contains("--"))
        .withColumn("floorSize", F.regexp_replace("floorSize", r"\D+", ""))
    )


def format_data_types(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.withColumn("numberOfRooms", F.col("numberOfRooms").cast("int"))
        .withColumn("numberOfBathroomsTotal", F.col("numberOfBathroomsTotal").cast("int"))
        .withColumn("numberOfBedrooms", F.col("numberOfBedrooms").cast("int"))
        .withColumn("price", F.col("price").cast("int"))
        .withColumn("floorSize", F.col("floorSize").cast("int"))
    )


def group_by_price_range_and_region(dataframe: DataFrame) -> DataFrame:
    df_with_bins = (
        dataframe.withColumn(
            "price_bin",
            F.when(F.col("price") < 100000, "<100K")
            .when((F.col("price") >= 100000) & (F.col("price") < 200000), "100K-200K")
            .when((F.col("price") >= 200000) & (F.col("price") < 300000), "200K-300K")
            .when((F.col("price") >= 300000) & (F.col("price") < 400000), "300K-400K")
            .when((F.col("price") >= 400000) & (F.col("price") < 500000), "400K-500K")
            .otherwise(">=500K"),
        )
        .groupBy("price_bin", "region")
        .count()
    )
    return df_with_bins


def rank_regions_by_price_per_sqm(dataframe: DataFrame) -> DataFrame:
    df = dataframe.withColumn("price_per_sqm", F.col("price") / F.col("floorSize"))

    grouped_df = df.groupBy("region").agg(
        F.format_number(F.avg("price_per_sqm"), 2).alias("mean_price_per_sqm")
    )

    windowSpec = Window.orderBy(F.col("mean_price_per_sqm").desc())

    ranked_df = grouped_df.withColumn("rank", F.rank().over(windowSpec))

    return ranked_df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.schema(object_schema)
        .json("/opt/spark/data/json_data", multiLine=True)
        .filter(F.col("@type") == "SellAction")
        .select("object.*")
    )

    df = (
        df.transform(preprocess_columns)
        .transform(extract_price_from_url)
        .transform(remove_bad_data)
        .transform(format_data_types)
    )

    query = (
        df.writeStream.queryName("mem_table")  # write to a memory table to make real time analysis
        .format("memory")
        .outputMode("append")
        .start()
    )

    query_parquet = (
        df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", "/opt/spark/data/stream")
        .option("checkpointLocation", "/opt/spark/data/checkpoint")
        .partitionBy("region")
        .start()
    )

    try:
        for _ in range(4):
            temp_df = spark.sql("SELECT * FROM mem_table")

            group_df = group_by_price_range_and_region(temp_df)
            rank_df = rank_regions_by_price_per_sqm(temp_df)

            print("Grouped Data:")
            group_df.show()
            print("Ranked Data:")
            rank_df.show()

            time.sleep(3)
        print("Stopping the streaming.")
        query.stop()
        query_parquet.stop()
        spark.stop()
    except KeyboardInterrupt:
        print("Stopping the streaming query")
        query.stop()
        query_parquet.stop()
        spark.stop()
