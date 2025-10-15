# =========================================================
# 🧩 Red Semafórica - Secretaría de Movilidad (SIMUR)
# Descarga los puntos de la red semafórica de Bogotá (GeoJSON via ArcGIS REST)
# Guarda en: data/raw/semaforos/semaforos_raw.parquet
# =========================================================

import os
import requests
import pandas as pd
from tqdm import tqdm

RAW_DIR = "data/raw/semaforos"
os.makedirs(RAW_DIR, exist_ok=True)

BASE_URL = "https://sig.simur.gov.co/arcgis/rest/services/DatosAbiertos/RedSemaforica/MapServer/0/query"


def fetch_semaforos(limit=10000):
    records = []
    offset = 0
    PAGE_SIZE = 2000
    with tqdm(total=limit, desc="Descargando semáforos") as pbar:
        while offset < limit:
            params = {
                "where": "1=1",
                "outFields": "*",
                "outSR": 4326,
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": PAGE_SIZE,
            }
            resp = requests.get(BASE_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            feats = data.get("features", [])
            if not feats:
                break
            for f in feats:
                attrs = f["attributes"]
                geom = f.get("geometry", {})
                attrs["lon"] = geom.get("x")
                attrs["lat"] = geom.get("y")
                records.append(attrs)
            offset += PAGE_SIZE
            pbar.update(len(feats))
            if len(feats) < PAGE_SIZE:
                break
    return pd.DataFrame(records)


def main():
    df = fetch_semaforos(limit=5000)
    print(f"✅ Registros descargados: {len(df)}")

    # Limpieza básica: coordenadas válidas
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["lat", "lon"])
    print(f"✅ Registros válidos con coordenadas: {len(df)}")

    out_path = os.path.join(RAW_DIR, "semaforos_raw.parquet")
    df.to_parquet(out_path, index=False)
    print(f"OK semáforos → {out_path}")


if __name__ == "__main__":
    main()
