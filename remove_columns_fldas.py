import pandas as pd

# Carregar o arquivo .parquet
number = 0

file_path = "./fldas/fldas_data_" + str(number) + "_new.parquet"
data = pd.read_parquet(file_path)

# Exibir as colunas do arquivo
print("Columns before modification:", data.columns)

# Remover as colunas desnecessárias
columns_to_keep = ["time", "lon", "lat", "Evap_tavg", "Tair_f_tavg"]  # Altere conforme necessário
data = data[columns_to_keep]

# Salvar o arquivo modificado
output_path = "./fldas/fldas_data_" + str(number) + "_new_" + str(number) +  "_novo.parquet"
data.to_parquet(output_path, index=False)
print(f"File saved at {output_path}")
