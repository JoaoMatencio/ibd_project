import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from datacore.utils.nasa import get_response, get_urls, load_xarray, write_parquet

URL = "https://cmr.earthdata.nasa.gov/search/granules"
stage = "wood"
collection = "nasa"
max_workers = 4


def process_url(url):
    try:
        df = load_xarray(get_response(url))
        df = df.to_dataframe()
        df = df.reset_index(level=["time", "Y", "X"])
        df = df.rename(columns={"Y": "lat", "X": "lon"})
        df["lat"] = df["lat"].apply(lambda x: round(x, 2))
        df["lon"] = df["lon"].apply(lambda x: round(x, 2))
        year = url.split("/")[-1].split(".")[1][1:5]
        month = int(url.split("/")[-1].split(".")[1][-2:])
        table = "fldas_land_surface_v2"
        prefixes = "/".join([f"year={year}", f"month={month}"])
        path = os.path.join(stage, collection, table, prefixes)
        file_name = "0000.parquet"
        key = os.path.join(path, file_name)
        write_parquet(path, file_name, df)
        print(f"File {key} saved")
    except Exception as e:
        print(f"Error processing {url} - {e}")


def load_fldas_land_surface(start_date, end_date):
    short_name = "FLDAS_NOAH01_C_GL_M"
    urls = get_urls(URL, short_name, start_date, end_date)
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_url, url) for url in urls]
        for future in as_completed(futures):
            results.append(future.result())
    return results


def __main__():
    load_fldas_land_surface("2000-06-01", "2024-12-31")


if __name__ == "__main__":
    __main__()
