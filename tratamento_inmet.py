from pathlib import Path
import pandas as pd

INPUT = Path("data")/"INMET_NE_PE_A370_SALGUEIRO_01-01-2024_A_31-12-2024.CSV"
OUTPUT_DIR = Path("Dados_Processados")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT = OUTPUT_DIR / "salgueiro_2024_tratado.csv"

df = pd.read_csv(INPUT, sep=';', encoding="latin1", header=8)

df = df[
    [
        "Data",
        "Hora UTC",
        "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
        "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)",
        "RADIACAO GLOBAL (Kj/m²)",
        "UMIDADE RELATIVA DO AR, HORARIA (%)",
    ]
].copy()

df["Hora UTC limpa"] = df["Hora UTC"].str.replace(" UTC", "", regex=False)
df["data_hora_utc"] = pd.to_datetime(df["Data"] + " "+ df["Hora UTC limpa"], format="%Y/%m/%d %H%M", errors = "coerce")

#troca virgula por ponto
def to_float_virgula(col: pd.Series) -> pd.Series:
    
    return pd.to_numeric(
        col.astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    )
num_cols = [
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)",
    "RADIACAO GLOBAL (Kj/m²)",
    "UMIDADE RELATIVA DO AR, HORARIA (%)",
]

for c in num_cols:
    df[c] = to_float_virgula(df[c])


df_limpo = df.dropna(subset=["TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)","PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)","RADIACAO GLOBAL (Kj/m²)","UMIDADE RELATIVA DO AR, HORARIA (%)","data_hora_utc"])

df_limpo = df_limpo.sort_values("data_hora_utc")

df_limpo = df_limpo.rename(columns={"TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "temperatura_c","PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressao_mb","RADIACAO GLOBAL (Kj/m²)": "radiacao_kj_m2","UMIDADE RELATIVA DO AR, HORARIA (%)": "umidade_relativa_pct","Data": "data","Hora UTC": "hora_utc"})

df_limpo.to_csv(OUTPUT, index= False)

print("Arquivo salvo em:", OUTPUT)