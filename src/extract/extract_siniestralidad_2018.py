# Uso: python -m src.extract.extract_siniestralidad_2018
# Descarga el Anuario 2018, selecciona hoja ACCIDENTES, normaliza encabezados y persiste parquet mínimo.

import os, io, re, unicodedata, requests, pandas as pd

RAW_DIR = "data/raw/siniestralidad_2018"
os.makedirs(RAW_DIR, exist_ok=True)

URL = "https://observatorio.movilidadbogota.gov.co/sites/default/files/2025-09/SIGAT_ANUARIO_2018_0.xlsx"

ADDR_PATTERNS = [
    r"(?i)direccion",
    r"(?i)dir_",
    r"(?i)_dir",
    r"(?i)TipoVia1",
    r"(?i)NumeroVia1",
    r"(?i)LetraVia1",
    r"(?i)CardinalVia1",
    r"(?i)TipoVia2",
    r"(?i)NumeroVia2",
    r"(?i)LetraVia2",
    r"(?i)CardinalVia2",
    r"(?i)Complemento",
]
DATE_PATTERNS = [r"(?i)fecha", r"(?i)mes_procesado", r"(?i)dia_procesado"]


def _normalize_cols(cols):
    out = []
    for c in cols:
        c = c.strip()
        c = "".join(
            ch
            for ch in unicodedata.normalize("NFD", c)
            if unicodedata.category(ch) != "Mn"
        )
        c = c.lower().replace(" ", "_").replace(".", "_").replace("/", "_")
        c = re.sub(r"__+", "_", c)
        out.append(c)
    return out


def _has_any(col, patterns):
    return any(re.search(p, col, flags=re.I) for p in patterns)


def main():
    bin_xlsx = requests.get(URL, timeout=180).content
    with open(os.path.join(RAW_DIR, "anuario_siniestralidad_2018.xlsx"), "wb") as f:
        f.write(bin_xlsx)

    xls = pd.ExcelFile(io.BytesIO(bin_xlsx))
    sheet = next(
        (s for s in xls.sheet_names if "ACCIDENT" in s.upper()), xls.sheet_names[0]
    )
    df = pd.read_excel(
        io.BytesIO(bin_xlsx), sheet_name=sheet, dtype=str, engine="openpyxl"
    )

    df.columns = _normalize_cols(df.columns)

    keep = []
    for c in df.columns:
        if _has_any(c, ADDR_PATTERNS) or _has_any(c, DATE_PATTERNS):
            keep.append(c)
    for c in ("localidad", "municipio", "latitud", "longitud"):
        if c in df.columns:
            keep.append(c)
    keep = sorted(set(keep)) or list(df.columns)

    out = df[keep].copy()
    for c in list(out.columns):
        if re.search(r"(?i)fecha", c):
            out[c] = pd.to_datetime(out[c], errors="coerce", dayfirst=True)

    out_path = os.path.join(RAW_DIR, "siniestralidad_2018_raw.parquet")
    out.to_parquet(out_path, index=False)
    print(
        f"OK siniestralidad 2018 → {out_path} ({len(out)} filas, {len(out.columns)} columnas)"
    )


if __name__ == "__main__":
    main()
