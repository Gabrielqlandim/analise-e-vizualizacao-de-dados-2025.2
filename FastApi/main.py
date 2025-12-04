from fastapi import FastAPI, UploadFile, File, HTTPException
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import os
from pathlib import Path
import requests
import pandas as pd
import time

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "inmet-raw")

TB_URL = os.getenv("THINGSBOARD_URL", "http://thingsboard:8080")  
TB_DEVICE_ID = "5fa1eb90-cfb6-11f0-b22f-fbaf6221b629"

app = FastAPI(title="Ingestão INMET MinIO")

def create_s3_client():
    #Cria um client S3/MinIO 
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket_exists(s3_client, bucket):
    #Garante que o bucket exista, sem derrubar o servidor se der erro
    try:
        #tenta ver se o bucket existe
        s3_client.head_bucket(Bucket=bucket)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        #se não existir, tenta criar
        if error_code in ("404", "NoSuchBucket", "404 Not Found"):
            try:
                s3_client.create_bucket(Bucket=bucket)

            except ClientError as e2:
                raise HTTPException(
                    status_code=500,
                    detail=f"Erro ao criar bucket no MinIO",
                )
        
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Erro ao acessar bucket no MinIO",
            )


@app.get("/")
def root():
    return {"status": "ok", "service": "fastapi-inmet"}



def upload_dataframe_to_minio(df, key: str):
    s3_client = create_s3_client()
    ensure_bucket_exists(s3_client, MINIO_BUCKET)

    # Converter DataFrame → CSV em memória
    csv_bytes = df.to_csv(index=False).encode("latin1")
    
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=csv_bytes,
        ContentType="text/csv"
    )

    return {
        "message": "DataFrame enviado para o MinIO com sucesso!",
        "bucket": MINIO_BUCKET,
        "key": key,
    }


@app.post("/inmet/ingest-local")
def ingest_local():
    
    #Lê um CSV local dentro do container e envia para o MinIO.
    
    local_path = Path("/data/INMET_NE_PE_A370_SALGUEIRO_01-01-2024_A_31-12-2024.CSV")

    if not local_path.exists():
        raise HTTPException(status_code=404, detail=f"Arquivo não encontrado")

    s3_client = create_s3_client()
    ensure_bucket_exists(s3_client, MINIO_BUCKET)

    #Cria o buket do mlflow
    ensure_bucket_exists(s3_client, "mlflow")

    with local_path.open("rb") as f:
        content = f.read()

    key = "raw/INMET_SALGUEIRO_2024.csv"

    try:
        s3_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=content,
            ContentType="text/csv",
        )
    except ClientError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao enviar arquivo local para o MinIO",
        )

    return {
        "message": "Arquivo local enviado para o MinIO com sucesso!",
        "bucket": MINIO_BUCKET,
        "key": key,
    }


def authenticate_thingsboard():
    url = f"{TB_URL}/api/auth/login"

    json = {
            "username": "tenant@thingsboard.org",
            "password": "tenant"
            }
    
    response = requests.post(url, json=json)

    if response.status_code != 200:
        raise Exception(f"Erro ao autenticar: {response.text}")
    
    data = response.json()

    token = data.get("token") or data.get("jwtToken")

    if not token:
        raise Exception("Token não encontrado na resposta do ThingsBoard.")

    return token


def get_device_id_by_name(device_name):

    jwt = authenticate_thingsboard()

    headers = {"X-Authorization": f"Bearer {jwt}"}

    url = f"{TB_URL}/api/tenant/devices?deviceName={device_name}"
    
    resp = requests.get(url, headers=headers)

    if resp.status_code != 200:
        raise Exception(f"Erro ao buscar device: {resp.text}")

    data = resp.json()
    return data["id"]["id"]


@app.get("/migrar_dados")
def ingest_from_thingsboard():

    device_id = get_device_id_by_name("Thermostat T2")

    #Endpoint de telemetria
    url = f"{TB_URL}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"


    jwt = authenticate_thingsboard()

    headers = {
        "X-Authorization": f"Bearer {jwt}",
        "accept": "application/json"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        raise HTTPException(500, f"Erro ao consultar ThingsBoard: {e}")

    data = resp.json()

    if not data:
        raise HTTPException(404, "Nenhuma telemetria disponível no ThingsBoard.")

    #Converter telemetria para DataFrame
    #Formato retornado: { "temperatura": [ {"ts": 123, "value": "28.1"} ] }
    registros = []

    for chave, valores in data.items():
        for ponto in valores:
            registros.append({
                "ts": ponto["ts"],
                chave: ponto["value"]
            })

    if not registros:
        raise HTTPException(404, "Não há dados válidos de telemetria.")

    df = pd.DataFrame(registros)

    #Consolidar múltiplas variáveis com a mesma timestamp
    df = df.groupby("ts").first().reset_index()

    return upload_dataframe_to_minio(df, "raw/INMET_SALGUEIRO_2024.csv")
