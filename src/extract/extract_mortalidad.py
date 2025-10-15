# Uso: python -m src.extract.extract_mortalidad
# Descarga mortalidad OSB (datos abiertos)
import os, pandas as pd, requests
from io import StringIO

RAW = "data/raw/mortalidad"
os.makedirs(RAW, exist_ok=True)

URL = "https://datosabiertos.bogota.gov.co/dataset/bded0839-a912-467d-94c5-8561dbefcb22/resource/a956ec09-e22b-4b84-aeb9-25702cf610a0/download/osb_evento_transporte.csv"


def main():
    resp = requests.get(URL, timeout=120)
    df = pd.read_csv(StringIO(resp.text), sep=";", encoding="utf-8", low_memory=False)
    df.to_parquet(f"{RAW}/mortalidad_raw.parquet", index=False)
    print("OK mortalidad → data/raw/mortalidad/mortalidad_raw.parquet", len(df))


if __name__ == "__main__":
    main()
