from utils import get_response, get_urls, load_xarray, write_parquet

URL = "https://cmr.earthdata.nasa.gov/search/granules"
stage = "wood"
collection = "nasa"
max_workers = 4

def process_url(url, path):
    load_xarray(get_response(url))
    df = (
        load_xarray(get_response(url))
        .to_dataframe()
        .reset_index(level=["time", "Y", "X"])
        .rename(columns={"Y": "lat", "X": "lon"})
    )
    df["lat"] = df["lat"].apply(lambda x: round(x, 2))
    df["lon"] = df["lon"].apply(lambda x: round(x, 2))

    write_parquet(path, df)


def load_fldas_land_surface(start_date, end_date):
    short_name = "FLDAS_NOAH01_C_GL_M"
    urls = get_urls(URL, short_name, start_date, end_date)
    paths = [f"fldas/fldas_data_{i}.parquet" for i in range(len(urls))]
    
    for url, path in zip(urls, paths):
        process_url(url, path)


def __main__():
    load_fldas_land_surface("2023-01-01", "2023-01-31")


if __name__ == "__main__":
    __main__()