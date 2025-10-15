# Uso: python -m src.transform.join_localidades
# Asigna LOCALIDAD a comparendos y siniestros por spatial join.

import os, pandas as pd, geopandas as gpd

RAW_DIR = "data/raw"
CLEAN_DIR = "data/clean"
os.makedirs(CLEAN_DIR, exist_ok=True)

IN_COMP = f"{RAW_DIR}/comparendos_2018.parquet"
IN_SIN = f"{CLEAN_DIR}/siniestralidad_2018_geocoded_google_parallel.parquet"
GEOLOC = "data/raw/localidades/localidades.geojson"

OUT_COMP = f"{CLEAN_DIR}/comparendos_2018_loc.parquet"
OUT_SIN = f"{CLEAN_DIR}/siniestralidad_2018_loc.parquet"


def _to_points(df, lon_col="lon", lat_col="lat"):
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[lon_col], df[lat_col]), crs=4326
    )
    return gdf


def main():
    comp = pd.read_parquet(IN_COMP)
    sin = pd.read_parquet(IN_SIN)

    comp = comp.dropna(subset=["lat", "lon"])
    sin = sin.dropna(subset=["lat", "lon"])

    gcomp = _to_points(comp)
    gsin = _to_points(sin)

    locs = gpd.read_file(GEOLOC).to_crs(4326)
    name_col = (
        "LOCALIDAD"
        if "LOCALIDAD" in locs.columns
        else [c for c in locs.columns if "LOCAL" in c.upper()][0]
    )
    locs[name_col] = locs[name_col].astype(str).str.upper().str.strip()

    jc = gpd.sjoin(
        gcomp, locs[[name_col, "geometry"]], predicate="within", how="left"
    ).drop(columns=["index_right"])
    js = gpd.sjoin(
        gsin, locs[[name_col, "geometry"]], predicate="within", how="left"
    ).drop(columns=["index_right"])

    jc = jc.rename(columns={name_col: "LOCALIDAD_JOIN"}).drop(columns=["geometry"])
    js = js.rename(columns={name_col: "LOCALIDAD_JOIN"}).drop(columns=["geometry"])

    jc.to_parquet(OUT_COMP, index=False)
    js.to_parquet(OUT_SIN, index=False)
    print(f"OK → {OUT_COMP} ({len(jc)})")
    print(f"OK → {OUT_SIN} ({len(js)})")


if __name__ == "__main__":
    main()
