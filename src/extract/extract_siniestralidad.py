# Uso: python -m src.extract.extract_siniestralidad
import os, pandas as pd, requests
from io import BytesIO

RAW = "data/raw"
os.makedirs(RAW, exist_ok=True)

# Ajusta a tu origen (xlsx o csv). Aquí ejemplo XLSX 2018 del anuario.
XLSX_URL = "https://datosabiertos.bogota.gov.co/dataset/8aa2f79c-5d32-4e6a-8eb3-a5af0ac4c172/resource/cd33be14-8c79-411b-9a8a-99bcddec231d/download/base-anuario-de-siniestralidad-2019.xlsx"


def main():
    # Buenas prácticas: persistir fuente cruda
    bin_xlsx = requests.get(XLSX_URL, timeout=120).content
    with open(f"{RAW}/anuario_2019.xlsx", "wb") as f:
        f.write(bin_xlsx)

    # Carga hoja(s) de accidentes 2018 (ajusta nombre exacto si difiere)
    xls = pd.ExcelFile(BytesIO(bin_xlsx))
    sheet = [s for s in xls.sheet_names if "ACCIDENTE" in s.upper()][0]
    df = pd.read_excel(
        BytesIO(bin_xlsx), sheet_name=sheet, dtype=str, engine="openpyxl"
    )
    df.to_parquet(f"{RAW}/siniestralidad_2018_raw.parquet", index=False)
    print("OK:", len(df), "rows → data/raw/siniestralidad_2018_raw.parquet")


if __name__ == "__main__":
    main()
