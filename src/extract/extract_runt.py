# Uso: python -m src.extract.extract_runt
# Descarga parque automotor RUNT (Socrata API)
import os, pandas as pd
from sodapy import Socrata
from tqdm import tqdm

RAW = "data/raw/runt"
os.makedirs(RAW, exist_ok=True)

DATASET_ID = "u3vn-bdcy"  # Parque automotor (RUNT 2.0)
client = Socrata("www.datos.gov.co", None, timeout=60)


def fetch_all(dataset_id, page_size=50000, max_rows=None):
    rows, offset = [], 0
    with tqdm(desc="RUNT", unit="rows") as pbar:
        while True:
            batch = client.get(dataset_id, limit=page_size, offset=offset)
            if not batch:
                break
            rows.extend(batch)
            offset += len(batch)
            pbar.update(len(batch))
            if max_rows and len(rows) >= max_rows:
                break
    return pd.DataFrame.from_records(rows)


def main():
    df = fetch_all(DATASET_ID, page_size=50000, max_rows=300000)
    df.to_parquet(f"{RAW}/runt_raw.parquet", index=False)
    print("OK RUNT → data/raw/runt/runt_raw.parquet", len(df))


if __name__ == "__main__":
    main()
