import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window


def load_listings_data(spark: SparkSession, path: str = "/opt/spark/data/json_data") -> DataFrame:
    dataframe = (
        spark.read.json(path, multiLine=True)
        .filter(F.col("@type") == "SellAction")
        .select("object.*")
    )
    return dataframe


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
    spark: SparkSession = SparkSession.builder.appName("SparkStreaming").getOrCreate()

    df = (
        load_listings_data(spark)
        .transform(preprocess_columns)
        .transform(extract_price_from_url)
        .transform(remove_bad_data)
        .transform(format_data_types)
    )

    group_df = group_by_price_range_and_region(df)

    rank_df = rank_regions_by_price_per_sqm(df)

    group_df.show()

    rank_df.show()

    spark.stop()
