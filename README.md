# IBD Project

## Descrição Geral

Este projeto tem como objetivo processar e analisar dados relacionados à precipitação e variáveis ambientais utilizando dados provenientes da NASA (como FLDAS e IMERG). Os scripts incluem funcionalidades para download, conversão e processamento de dados, além de preparação de análises em SQL.

## Estrutura de Diretórios

A estrutura principal do projeto é a seguinte:

```
ibd_project/
├── fldas/
│   └── fldas_data.json
├── imerg/
│   └── imerg_data.json
├── processed/
│   ├── [Arquivos de dados processados em formato HDF5]
│   ├── extract.py
│   └── variaveis_fldas_imerg.json
├── Análises.sql
├── change_to_csv_fldas.py
├── change_to_csv_imerg.py
├── download_nasa.py
├── download_precipitation_nasa.py
├── process.py
├── remove_columns_fldas.py
├── remove_columns_imerg.py
├── utils.py
```

## Arquivos Principais

- **`Análises.sql`**: Scripts SQL para análise dos dados processados.
- **`change_to_csv_fldas.py` e `change_to_csv_imerg.py`**: Scripts para converter dados FLDAS e IMERG em arquivos CSV.
- **`download_nasa.py` e `download_precipitation_nasa.py`**: Scripts para realizar o download dos dados da NASA.
- **`process.py`**: Script principal para processamento dos dados.
- **`remove_columns_fldas.py` e `remove_columns_imerg.py`**: Scripts para remoção de colunas desnecessárias nos dados FLDAS e IMERG.
- **`utils.py`**: Funções utilitárias utilizadas em outros scripts.

## Dependências e Pré-requisitos

Antes de executar o projeto, certifique-se de ter:

1. Python 3.8 ou superior instalado.
2. Bibliotecas listadas no arquivo `requirements.txt` (se houver). Caso contrário, instale as principais bibliotecas:

```bash
pip install pandas numpy requests h5py
```

## Como Executar

1. Clone este repositório:

```bash
git clone [URL_DO_REPOSITORIO]
cd ibd_project
```

2. Execute o script de download para obter os dados:

```bash
python download_nasa.py
```

3. Converta os dados para CSV (se necessário):

```bash
python change_to_csv_fldas.py
python change_to_csv_imerg.py
```

4. Processe os dados:

```bash
python process.py
```

5. Use os scripts SQL para realizar as análises desejadas.

## Contribuição

Sinta-se à vontade para contribuir com melhorias para este projeto. Para isso:

1. Fork este repositório.
2. Crie uma branch para sua feature ou correção:

```bash
git checkout -b minha-feature
```

3. Envie um pull request para revisão.

