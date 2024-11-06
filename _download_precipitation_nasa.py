import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from datacore.utils.nasa import (
    generate_month_ranges,
    get_dataframe,
    get_response,
    get_urls,
    load_mxarray,
    write_parquet,
)


def load_imerg_late_task(first_day, last_day, URL, short_name, stage, collection, table_name):
    try:
        start_str = first_day.strftime("%Y-%m-%d")
        end_str = last_day.strftime("%Y-%m-%d")

        content = get_response(get_urls(URL, short_name, start_str, end_str)[0])
        ds = load_mxarray([content], engine="h5netcdf", group="Grid", decode_times=False)
        df = get_dataframe(ds)

        prefixes = "/".join([f"year={first_day.year}", f"month={first_day.month}"])
        path = os.path.join(stage, collection, table_name, prefixes)
        file_name = "0000.parquet"

        write_parquet(path, file_name, df)

        return {"status": "success", "month": f"{first_day.year}-{first_day.month}"}
    except Exception as e:
        return {"status": "error", "month": f"{first_day.year}-{first_day.month}", "error": str(e)}


def load_imerg_late(start_date, end_date):
    URL = "https://cmr.earthdata.nasa.gov/search/granules"
    stage = "wood"
    collection = "nasa"
    short_name = "GPM_3IMERGM"
    table_name = "imerg_v7_final"

    date_ranges = generate_month_ranges(start_date, end_date)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(
                load_imerg_late_task, first_day, last_day, URL, short_name, stage, collection, table_name
            )
            for first_day, last_day in date_ranges
        ]
        results = [future.result() for future in as_completed(futures)]

    return {"statusCode": 200, "body": "Finished loading files to s3", "details": results}


def __main__():
    load_imerg_late("2000-06-01", "2000-06-30")


if __name__ == "__main__":
    __main__()
