# Uso: python -m src.transform.geocode_addresses
import os, json, hashlib, time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

RAW, CLEAN, WORK = "data/raw", "data/clean", "data/working"
os.makedirs(CLEAN, exist_ok=True)
os.makedirs(WORK, exist_ok=True)

CACHE = f"{WORK}/geocode_cache.json"


def sha(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def build_address(row) -> str:
    # Buenas prácticas: dirección + localidad + ciudad/país
    for c in ["Direccion", "DIRECCION", "direccion"]:
        if c in row and isinstance(row[c], str) and row[c].strip():
            base = row[c].strip()
        else:
            continue
        loc = (row.get("Localidad") or row.get("LOCALIDAD") or "").strip()
        suf = f"{', ' + loc if loc else ''}, Bogotá, Colombia"
        return (base + suf).strip(", ")
    return None


def main():
    df = pd.read_parquet(f"{RAW}/siniestralidad_2018_raw.parquet")
    if "Latitud" in df.columns and "Longitud" in df.columns:
        # conservar coords válidas; geocodificar las 0/NaN
        df["lat"] = pd.to_numeric(df["Latitud"], errors="coerce")
        df["lon"] = pd.to_numeric(df["Longitud"], errors="coerce")
        df.loc[df["lat"].eq(0) | df["lon"].eq(0), ["lat", "lon"]] = None
    else:
        df["lat"] = None
        df["lon"] = None

    df["addr"] = df.apply(build_address, axis=1)
    todo = df[df["lat"].isna() | df["lon"].isna()].copy()

    cache = {}
    if os.path.exists(CACHE):
        cache = json.loads(open(CACHE, "r", encoding="utf-8").read())

    geoloc = Nominatim(user_agent="etl-bogota", timeout=15)
    geocode = RateLimiter(
        geoloc.geocode, min_delay_seconds=1.1, swallow_exceptions=True
    )

    for a in todo["addr"].dropna().drop_duplicates():
        k = sha(a)
        if k in cache:
            continue
        loc = geocode(a, country_codes="co", language="es")
        cache[k] = {
            "lat": (loc.latitude if loc else None),
            "lon": (loc.longitude if loc else None),
            "q": a,
        }
        if len(cache) % 50 == 0:
            open(CACHE, "w", encoding="utf-8").write(
                json.dumps(cache, ensure_ascii=False)
            )

    open(CACHE, "w", encoding="utf-8").write(json.dumps(cache, ensure_ascii=False))

    def pick(a, f):
        if not isinstance(a, str):
            return None
        r = cache.get(sha(a)) or {}
        return r.get(f)

    df.loc[df["lat"].isna(), "lat"] = df.loc[df["lat"].isna(), "addr"].map(
        lambda a: pick(a, "lat")
    )
    df.loc[df["lon"].isna(), "lon"] = df.loc[df["lon"].isna(), "addr"].map(
        lambda a: pick(a, "lon")
    )

    # Filtro BBOX Bogotá
    bbox = (-74.25, 4.45, -73.95, 4.90)
    m = df["lat"].between(bbox[1], bbox[3]) & df["lon"].between(bbox[0], bbox[2])
    df = df[m | df["lat"].isna() | df["lon"].isna()]
    df.to_parquet(f"{CLEAN}/siniestralidad_2018_geocoded.parquet", index=False)
    print("OK:", len(df), "→ data/clean/siniestralidad_2018_geocoded.parquet")


if __name__ == "__main__":
    main()
