# Uso: python -m src.extract.extract_comparendos
import os, requests
import pandas as pd
from tqdm import tqdm

RAW = "data/raw"
os.makedirs(RAW, exist_ok=True)

BASE = "https://services2.arcgis.com/NEwhEo9GGSHXcRXV/arcgis/rest/services/ComparendosDEI2018/FeatureServer/0/query"


def fetch(limit=200000, page=2000):
    out, offset = [], 0
    with tqdm(desc="comparendos 2018", total=limit) as pbar:
        while offset < limit:
            params = {
                "where": "1=1",
                "outFields": "*",
                "outSR": 4326,
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": page,
                "returnGeometry": "true",
            }
            r = requests.get(BASE, params=params, timeout=60)
            r.raise_for_status()
            feats = r.json().get("features", [])
            if not feats:
                break
            for f in feats:
                a = f.get("attributes", {})
                g = f.get("geometry") or {}
                a["lon"], a["lat"] = g.get("x"), g.get("y")
                out.append(a)
            offset += len(feats)
            pbar.update(len(feats))
            if len(feats) < page:
                break
    return pd.DataFrame(out)


def main():
    df = fetch()
    # Buenas prácticas: tipos y fechas consistentes
    if "FECHA_HORA" in df.columns:
        df["FECHA_HORA"] = pd.to_datetime(df["FECHA_HORA"], unit="ms", errors="coerce")
    df = df.dropna(subset=["lat", "lon"])
    df.to_parquet(f"{RAW}/comparendos_2018.parquet", index=False)
    print("OK:", len(df), "rows → data/raw/comparendos_2018.parquet")


if __name__ == "__main__":
    main()
