from fastapi import FastAPI, UploadFile, File, HTTPException
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import os
from pathlib import Path

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "inmet-raw")


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


def ensure_bucket_exists(s3_client):
    #Garante que o bucket exista, sem derrubar o servidor se der erro
    try:
        #tenta ver se o bucket existe
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        #se não existir, tenta criar
        if error_code in ("404", "NoSuchBucket", "404 Not Found"):
            try:
                s3_client.create_bucket(Bucket=MINIO_BUCKET)
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


app = FastAPI(title="Ingestão INMET MinIO")


@app.get("/")
def root():
    return {"status": "ok", "service": "fastapi-inmet"}


@app.post("/inmet/ingest-file")
async def ingest_file(file: UploadFile = File(...)):
    
    #Recebe um CSV via upload e salva no MinIO, no bucket inmet-raw.
    
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Envie um arquivo .csv")

    s3_client = create_s3_client()
    ensure_bucket_exists(s3_client)

    content = await file.read()
    key = f"raw/{file.filename}"

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
            detail=f"Erro ao enviar arquivo para o MinIO",
        )

    return {
        "message": "Arquivo enviado para o MinIO com sucesso!",
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
    ensure_bucket_exists(s3_client)

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
