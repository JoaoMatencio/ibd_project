import h5py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


number = 12

# Caminho do arquivo HDF5
file_path = "./3B-MO.MS.MRG.3IMERG.2023" + str(number) + "01-S000000-E235959." + str(number) + ".V07B.HDF5"
output_path = "./../imerg/imerg_data_" + str(number) + "_new_" + str(number) +  "_novo.parquet"

# Data base para a conversão de tempo
BASE_TIME = datetime(1980, 1, 6)

# Abrir o arquivo HDF5
with h5py.File(file_path, 'r') as hdf:
    print("Chaves principais no arquivo:")
    grid_group = hdf["Grid"]
    print(list(grid_group.keys()))

    # Coordenadas espaciais
    lon = grid_group["lon"][:]  # Longitudes
    lat = grid_group["lat"][:]  # Latitudes

    # Criar meshgrid para latitude e longitude
    lon_grid, lat_grid = np.meshgrid(lon, lat)

    # Processar o tempo
    time = grid_group["time_bnds"][:]  # Variável de tempo em segundos desde BASE_TIME
    num_lon = len(lon)
    num_lat = len(lat)

    if len(time) == 1:
        # Apenas um valor de tempo, replicar para todas as combinações espaciais
        time_column = np.full(
            lat_grid.size,
            pd.Timestamp(BASE_TIME + timedelta(seconds=int(time[0][0]))).to_numpy()
        )
    else:
        # Múltiplos valores de tempo, expandir para cobrir todas as combinações
        time_expanded = np.repeat(
            [pd.Timestamp(BASE_TIME + timedelta(seconds=int(t))).to_numpy() for t in time.flatten()],
            num_lon * num_lat
        )
        time_column = time_expanded

    # Inicializar DataFrame com o tempo como a primeira coluna
    data = {
        "time_bnds": time_column,
        "latitude": np.tile(lat_grid.flatten(), len(time)),  # Repete latitudes para cada tempo
        "longitude": np.tile(lon_grid.flatten(), len(time)),  # Repete longitudes para cada tempo
    }

    # Processar variáveis tridimensionais
    for key in ["precipitation", "randomError", "gaugeRelativeWeighting",
                "probabilityLiquidPrecipitation", "precipitationQualityIndex"]:
        if key in grid_group:
            dataset = grid_group[key][:]
            if dataset.ndim == 3:  # Remover dimensão redundante
                dataset = dataset.squeeze()
            data[key] = dataset.flatten()  # Achatar os dados

    # Criar DataFrame
    df = pd.DataFrame(data)

    # Converter a coluna "time" para datetime64[ms]
    df["time_bnd"] = df["time_bnds"].astype("datetime64[ms]")

    # Salvar como Parquet
    df.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"Arquivo convertido e salvo como {output_path}")
