import pandas as pd

# Converter arquivo Parquet para CSV
number = 0 
parquet_file = "./fldas/fldas_data_" + str(number) + "_new_" + str(number) + "_novo.parquet"
csv_file = "./fldas/fldas_data_" + str(number) + ".csv"
df = pd.read_parquet(parquet_file)
df.to_csv(csv_file, index=False)


