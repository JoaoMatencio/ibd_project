import pandas as pd

# Carregar o arquivo .parquet
number = 1

file_path = "./imerg/imerg_data_" + str(number) + "_new_" + str(number) +  "_novo.parquet"
data = pd.read_parquet(file_path)

# Exibir as colunas do arquivo
print("Columns before modification:", data.columns)

# Remover as colunas desnecessárias
columns_to_keep = ["time_bnds", "latitude", "longitude","precipitation"]  # Altere conforme necessário
data = data[columns_to_keep]

# Salvar o arquivo modificado
output_path = "./imerg/imerg_data_" + str(number) +  ".parquet"
data.to_parquet(output_path, index=False)
print(f"File saved at {output_path}")
