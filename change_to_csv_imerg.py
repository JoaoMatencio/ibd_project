import pandas as pd

# Converter arquivo Parquet para CSV
number = 12
parquet_file = "./imerg/imerg_data_" + str(number) + ".parquet"
csv_file = "./imerg/imerg_data_" + str(number) + ".csv"
df = pd.read_parquet(parquet_file)
df.to_csv(csv_file, index=False)


