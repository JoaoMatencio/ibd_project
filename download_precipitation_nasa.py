from utils import (
    generate_month_ranges,
    get_dataframe,
    get_response,
    get_urls,
    load_mxarray,
    write_parquet,
)


def load_imerg_late_task(first_day, last_day, URL, short_name, path):
    start_str = first_day.strftime("%Y-%m-%d")
    end_str = last_day.strftime("%Y-%m-%d")
    content = get_response(get_urls(URL, short_name, start_str, end_str)[0])
    ds = load_mxarray([content], engine="h5netcdf", group="Grid", decode_times=False)
    df = get_dataframe(ds)
    write_parquet(path, df)


def load_imerg_late(start_date, end_date):
    URL = "https://cmr.earthdata.nasa.gov/search/granules"
    short_name = "GPM_3IMERGM"
    date_ranges = generate_month_ranges(start_date, end_date)
    paths = [f"imerg/imerg_data_{ix}.parquet" for ix, _ in enumerate(date_ranges)]
    
    for (first_day, last_day), path in zip(date_ranges, paths):
        load_imerg_late_task(first_day, last_day, URL, short_name, path)



def __main__():
    load_imerg_late("2023-01-01", "2023-12-30")


if __name__ == "__main__":
    __main__()
