FROM ghcr.io/mlflow/mlflow:v3.6.0

# Instalar dependências necessárias para compilar psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar psycopg2 (versão binária recomendada)
RUN pip install --no-cache-dir psycopg2-binary boto3 pymysql

RUN mkdir -p /mlflow/

WORKDIR /mlflow

RUN echo 
