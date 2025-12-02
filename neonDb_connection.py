import pandas as pd
from minio import Minio
from sqlalchemy import create_engine

def minio_neon():
    minio_client = Minio("localhost:9000", access_key= "minioadmin", secret_key= "minioadmin", secure=False)

    minio_client.fget_object("inmet-raw","raw/INMET_SALGUEIRO_2024.csv", "arquivo_temp_minio_inmet.csv")
    df = pd.read_csv("arquivo_temp_minio_inmet.csv", sep=';', encoding="latin1", header=8)

    string_conexao = "postgresql://neondb_owner:npg_ZyDj34mdtqLu@ep-crimson-glitter-ac9d92fb-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    criacao_engine = create_engine(string_conexao)
    
    df.to_sql(
    "inmet_bruto",
    criacao_engine,
    if_exists="append",
    index=False
    )

minio_neon()