def water_balance(group_df):
    group_df = group_df.sort_values(by="date").reset_index(drop=True)
    group_df["disponibilidade_hidrica"] = 0.0

    for ix in range(len(group_df)):
        if ix == 0:
            group_df.at[ix, "disponibilidade_hidrica"] = min(group_df.at[ix, "balanco_hidrico"], 100.0)
        else:
            prev_disponibilidade = group_df.at[ix - 1, "disponibilidade_hidrica"]
            new_disponibilidade = prev_disponibilidade + group_df.at[ix, "balanco_hidrico"]
            group_df.at[ix, "disponibilidade_hidrica"] = min(new_disponibilidade, 100.0)

    return group_df


def get_schema():
    return """
        lon double,
        lat double,
        year int,
        month int,
        precipitation float,
        LWdown_f_tavg float,
        Lwnet_tavg float,
        Psurf_f_tavg float,
        Qair_f_tavg float,
        Qg_tavg float,
        Qh_tavg float,
        Qle_tavg float,
        Qs_tavg float,
        Qsb_tavg float,
        RadT_tavg float,
        Rainf_f_tavg float,
        SWE_inst float,
        SWdown_f_tavg float,
        SnowCover_inst float,
        SnowDepth_inst float,
        Snowf_tavg float,
        Swnet_tavg float,
        Tair_f_tavg float,
        Wind_f_tavg float,
        SoilMoi00_10cm_tavg float,
        SoilMoi10_40cm_tavg float,
        SoilMoi40_100cm_tavg float,
        SoilMoi100_200cm_tavg float,
        temp_00_10_cm float,
        temp_10_40_cm float,
        temp_40_100_cm float,
        temp_100_200_cm float,
        date long,
        evapotranspiracao float,
        balanco_hidrico float,
        disponibilidade_hidrica double
    """


def get_columns_to_drop():
    return [
        "nv",
        "lonv",
        "latv",
        "time_bnds",
        "lon_bnds",
        "lat_bnds",
        "time",
        "Evap_tavg",
        "SoilTemp00_10cm_tavg",
        "SoilTemp10_40cm_tavg",
        "SoilTemp40_100cm_tavg",
        "SoilTemp100_200cm_tavg",
    ]


def __main__():
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession

    local = False

    lat_min = -33.75  # Southernmost point of Brazil (Chuí, RS)
    lat_max = 5.27  # Northernmost point of Brazil (Monte Caburaí, RR)
    lon_min = -73.99  # Westernmost point of Brazil (Mâncio Lima, AC)
    lon_max = -34.79  # Easternmost point of Brazil (Ponta do Seixas, PB)
    kelvin = 273.15  # Kelvin to Celsius
    precipitation_factor = 1000  # cm³ to mm³
    evapotranspiration_factor = 2592000  # ss to month

    path_imerg = #TODO ADD Path
    path_land = #TODO ADD Path
    path_processed = #TODO ADD Path

    spark = (
        SparkSession.builder.appName("nasa-climate")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )

    land_surface = (
        spark.read.parquet(path_land)
        .filter("Evap_tavg is not null")
        .filter(f"lon >= {lon_min} AND lon <= {lon_max}")
        .filter(f"lat >= {lat_min} AND lat <= {lat_max}")
        .dropDuplicates(subset=["lon", "lat", "year", "month"])
    )

    imerg = (
        spark.read.parquet(path_imerg)
        .filter("precipitation is not null")
        .filter(f"lon >= {lon_min} AND lon <= {lon_max} AND lat >= {lat_min} AND lat <= {lat_max}")
        .withColumn("lat", F.round(F.col("lat").cast("double"), 2))
        .withColumn("lon", F.round(F.col("lon").cast("double"), 2))
        .groupBy("lon", "lat", "year", "month")
        .agg(F.first("precipitation").alias("precipitation"))
    )

    (
        imerg.join(land_surface, on=["lon", "lat", "year", "month"], how="inner")
        .withColumn("date", F.unix_timestamp(F.col("time"), "yyyy-MM-dd"))
        .withColumn("precipitation", F.col("precipitation") * precipitation_factor)
        .withColumn("evapotranspiracao", F.col("Evap_tavg") * evapotranspiration_factor)
        .withColumn("balanco_hidrico", F.col("precipitation") - F.col("evapotranspiracao"))
        .withColumn("temp_00_10_cm", F.col("SoilTemp00_10cm_tavg") - kelvin)
        .withColumn("temp_10_40_cm", F.col("SoilTemp10_40cm_tavg") - kelvin)
        .withColumn("temp_40_100_cm", F.col("SoilTemp40_100cm_tavg") - kelvin)
        .withColumn("temp_100_200_cm", F.col("SoilTemp100_200cm_tavg") - kelvin)
        .drop(*get_columns_to_drop())
        .groupBy("lat", "lon")
        .applyInPandas(water_balance, schema=get_schema())
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(path_processed)
    )


if __name__ == "__main__":
    __main__()
