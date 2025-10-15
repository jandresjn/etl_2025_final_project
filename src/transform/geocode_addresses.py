# src/transform/geocode_addresses_google_parallel.py

import os
import re
import json
import time
import hashlib
import pandas as pd
from tqdm import tqdm
from typing import Dict, Optional
from dotenv import load_dotenv
import concurrent.futures

# cargar .env
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

# paths
RAW_IN = "data/raw/siniestralidad_2018/siniestralidad_2018_raw.parquet"
CLEAN_DIR = "data/clean"
WORK_DIR = "data/working"
CACHE_FN = os.path.join(WORK_DIR, "geocache_google.json")
OUT_FN = os.path.join(CLEAN_DIR, "siniestralidad_2018_geocoded_google_parallel.parquet")

os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(WORK_DIR, exist_ok=True)

# config
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
if not GOOGLE_API_KEY:
    raise ValueError("No se encontró GOOGLE_MAPS_API_KEY en .env")

BBOX = (-74.25, 4.45, -73.95, 4.90)  # Bogotá
MAP_TIPO = {
    "CL": "CALLE",
    "KR": "CARRERA",
    "CR": "CARRERA",
    "AK": "AVENIDA CARRERA",
    "AC": "AVENIDA CALLE",
    "AV": "AVENIDA",
    "DG": "DIAGONAL",
    "TV": "TRANSVERSAL",
    "AUT": "AUTOPISTA",
}
MAP_CARD = {"N": "NORTE", "S": "SUR", "E": "ESTE", "O": "OESTE", "W": "OESTE"}


# ============================ helpers ============================
def _sha(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _clean(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    return re.sub(r"\s+", " ", s)


def _fmt_via(tipo, num, letra, card):
    if not (tipo or num):
        return ""
    tipo2 = MAP_TIPO.get(_clean(tipo), _clean(tipo))
    base = " ".join([p for p in [tipo2, f"{_clean(num)}{_clean(letra)}"] if p])
    card2 = MAP_CARD.get(_clean(card), _clean(card))
    return (f"{base} {card2}".strip() if card2 else base).strip()


def _addr_core(row: pd.Series) -> Optional[str]:
    p1 = _fmt_via(
        row.get("tipovia1"),
        row.get("numerovia1"),
        row.get("letravia1"),
        row.get("cardinalvia1"),
    )
    p2 = _fmt_via(
        row.get("tipovia2"),
        row.get("numerovia2"),
        row.get("letravia2"),
        row.get("cardinalvia2"),
    )
    base = f"{p1} CON {p2}" if p1 and p2 else (p1 or p2)
    if not base:
        for c in (
            "direccion",
            "direccion_normalizada",
            "dir",
            "direccion_accidente",
            "direccion_sitio",
        ):
            if c in row and isinstance(row[c], str) and row[c].strip():
                base = _clean(row[c])
                break
    if not base:
        return None
    loc = _clean(row.get("localidad") or row.get("LOCALIDAD") or "")
    return (base + (f", {loc}" if loc else "") + ", BOGOTÁ, COLOMBIA").strip(", ")


def _in_bbox(lat, lon) -> bool:
    if pd.isna(lat) or pd.isna(lon):
        return False
    return (BBOX[1] <= lat <= BBOX[3]) and (BBOX[0] <= lon <= BBOX[2])


def _load_cache() -> Dict[str, Dict]:
    if os.path.exists(CACHE_FN):
        try:
            with open(CACHE_FN, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_cache(cache: Dict[str, Dict]) -> None:
    with open(CACHE_FN, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ============================ geocoding ============================
def geocode_one(address: str):
    from googlemaps import Client

    gmaps = Client(key=GOOGLE_API_KEY)
    try:
        res = gmaps.geocode(address, region="co")
        if res:
            loc = res[0]["geometry"]["location"]
            formatted = res[0].get("formatted_address")
            return (address, loc.get("lat"), loc.get("lng"), formatted)
    except Exception:
        return (address, None, None, None)
    return (address, None, None, None)


def geocode_parallel(addresses, max_workers=10):
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for addr, lat, lon, fmt in tqdm(
            executor.map(geocode_one, addresses),
            total=len(addresses),
            desc="Google geocode",
        ):
            results[addr] = (lat, lon, fmt)
            if len(results) % 50 == 0:  # guardado incremental
                _save_cache(
                    cache
                    | {
                        _sha(k): {"q": k, "lat": v[0], "lon": v[1], "addr": v[2]}
                        for k, v in results.items()
                    }
                )
                time.sleep(0.05)
    return results


# ============================ main ============================
def main():
    # 🚫 Si ya existe el parquet final, no hacer nada
    if os.path.exists(OUT_FN):
        print(f"✅ Archivo final ya existe: {OUT_FN}\nNada que hacer.")
        return

    if not os.path.exists(RAW_IN):
        raise FileNotFoundError(f"No existe entrada: {RAW_IN}")

    df = pd.read_parquet(RAW_IN)
    df.columns = [c.strip().lower() for c in df.columns]

    if "latitud" in df.columns and "longitud" in df.columns:
        df["lat"] = pd.to_numeric(df["latitud"], errors="coerce")
        df["lon"] = pd.to_numeric(df["longitud"], errors="coerce")
        df.loc[(df["lat"] == 0) | (df["lon"] == 0), ["lat", "lon"]] = pd.NA
    else:
        df["lat"] = pd.NA
        df["lon"] = pd.NA

    df["addr_core"] = df.apply(_addr_core, axis=1)
    need = df[(df["lat"].isna()) | (df["lon"].isna())]
    queries = need["addr_core"].dropna().drop_duplicates()
    queries = [q for q in queries if isinstance(q, str) and len(q) >= 8]

    cache = _load_cache()

    # 🚫 Si todas las direcciones ya están en cache, no llamar API
    all_in_cache = all(_sha(q) in cache for q in queries)
    if all_in_cache:
        print(f"✅ Cache completa ({len(cache)} direcciones). Nada que hacer.")
    else:
        missing = [q for q in queries if _sha(q) not in cache]
        print("Total a geocodificar:", len(missing))
        if missing:
            results = geocode_parallel(missing, max_workers=8)
            for q, (lat, lon, fmt) in results.items():
                cache[_sha(q)] = {"q": q, "lat": lat, "lon": lon, "addr": fmt}
            _save_cache(cache)

    # aplicar resultados
    def _pick(a, key):
        return cache.get(_sha(a), {}).get(key, pd.NA)

    df["lat_fill"] = df["addr_core"].map(lambda a: _pick(a, "lat"))
    df["lon_fill"] = df["addr_core"].map(lambda a: _pick(a, "lon"))
    df["geocode_address"] = df["addr_core"].map(lambda a: _pick(a, "addr"))
    df.loc[df["lat"].isna(), "lat"] = df["lat_fill"]
    df.loc[df["lon"].isna(), "lon"] = df["lon_fill"]

    mask_ok = df.apply(
        lambda r: (
            _in_bbox(r["lat"], r["lon"])
            if pd.notna(r["lat"]) and pd.notna(r["lon"])
            else False
        ),
        axis=1,
    )
    df.loc[~mask_ok, ["lat", "lon", "geocode_address"]] = pd.NA

    df.to_parquet(OUT_FN, index=False)
    total_ok = int((df["lat"].notna() & df["lon"].notna()).sum())
    print(f"\n✅ Guardado en: {OUT_FN}")
    print(f"📍 Coordenadas válidas: {total_ok} / {len(df)}")


if __name__ == "__main__":
    main()
