import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv('dados_processados/salgueiro_2024_tratado.csv', parse_dates=["data_hora_utc"])

#aqui é pra so levar ao banco de dados as tabelas que importam pra gente tratar
df = df[[
    "data_hora_utc",
    "temperatura_c",
    "pressao_mb",
    "radiacao_kj_m2",
    "umidade_relativa_pct",
]]

user = "nenondb"
password = "neondb_owner"
host = "pg.neon.tech"
porta = 5432
nome_banco = "analise e vizualizacao de dados"

string_conexao = "postgresql://neondb_owner:npg_ZyDj34mdtqLu@ep-crimson-glitter-ac9d92fb-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

criacao_engine = create_engine(string_conexao)

df.to_sql(
    "medidas_salgueiro",
    criacao_engine,
    if_exists="append",
    index=False
)

print("Foi inserido no bd")