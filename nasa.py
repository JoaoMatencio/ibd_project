import datetime
import io
import os
import urllib
from dataclasses import dataclass
from http.cookiejar import CookieJar

import boto3
import pandas as pd
import requests
import xarray as xr


def get_ssm_parameter(parameter_name):
    ssm_client = boto3.client("ssm")
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=False)
    return response["Parameter"]["Value"]


username = get_ssm_parameter("/nasa/account")
password = get_ssm_parameter("/nasa/password")
password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
password_manager.add_password(None, "https://urs.earthdata.nasa.gov", username, password)

cookie_jar = CookieJar()
opener = urllib.request.build_opener(
    urllib.request.HTTPBasicAuthHandler(password_manager),
    urllib.request.HTTPCookieProcessor(cookie_jar),
)
urllib.request.install_opener(opener)


@dataclass
class Storage:
    USER = os.path.expanduser("~")
    dlake_bucket = "bt-dlake-dev"
    s3_uri = "s3://"


def load_xarray(content, **xr_kwargs):
    bytes_like = io.BytesIO(content)
    ds = xr.open_dataset(bytes_like, **xr_kwargs)
    return ds


def write_s3_file(bucket, key, data):
    client = boto3.client("s3")
    client.put_object(Bucket=bucket, Key=key, Body=data)


def write_parquet(path, file_name, data: pd.DataFrame, local=False):
    if local:
        path_dir = os.path.join(Storage.USER, Storage.dlake_bucket, path)
        file_path = os.path.join(path_dir, file_name)
        os.makedirs(path_dir, exist_ok=True)
        data.to_parquet(file_path, coerce_timestamps="ms", index=False)
    else:
        file_path = os.path.join(path, file_name)
        print(file_path)
        out_buffer = io.BytesIO()
        data.to_parquet(out_buffer, coerce_timestamps="ms", index=False)
        write_s3_file(Storage.dlake_bucket, file_path, out_buffer.getvalue())


def cmr_request(url, params):
    response = requests.get(url, params=params, headers={"Accept": "application/json"})
    if response.status_code != 200:
        print(f"{response.status_code}, CMR is not accessible, check for outages")
        return None
    print(f"{response.status_code}, CMR is accessible")
    return response


def get_urls(url, short_name, start_date, end_date):
    start_time = datetime.datetime.strptime(start_date, "%Y-%m-%d").isoformat()
    last_hour = end_date + " 23:59:59"
    end_time = datetime.datetime.strptime(last_hour, "%Y-%m-%d %H:%M:%S").isoformat()

    response = cmr_request(
        url, {"short_name": short_name, "temporal": start_time + "," + end_time, "page_size": 2000}
    )
    if not response:
        return []
    granules = response.json()["feed"]["entry"]
    return [
        next((item["href"] for item in granule["links"] if item["href"].startswith("https")), None)
        for granule in granules
    ]


def get_response(url):
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request)
    if response.getcode() == 200:
        return response.read()
    raise ConnectionError(f"Request returned the error code {response.getcode()}")


def load_mxarray(content, **xr_kwargs):
    bytes_like = [io.BytesIO(c) for c in content]
    return xr.open_mfdataset(bytes_like, **xr_kwargs)


def get_dataframe(dataset: xr.Dataset) -> pd.DataFrame:
    df = dataset.to_dataframe().reset_index(level=["time", "lon", "lat"])
    df["lat"] = df["lat"].apply(lambda x: round(x, 2))
    df["lon"] = df["lon"].apply(lambda x: round(x, 2))
    df["time"] = df["time"].astype("datetime64[ms]")
    return df


def generate_month_ranges(start_date, end_date):
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    months = pd.date_range(start=start, end=end, freq="MS")
    return [(month, month + pd.offsets.MonthEnd(0)) for month in months]


def exists_in_s3(object_key, bucket_name=Storage.dlake_bucket):
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except Exception as _:
        return False
