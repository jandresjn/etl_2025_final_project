# Uso: python -m src.transform.calc_proximidad_semaforos
# Calcula distancia al semáforo más cercano (m) y buckets.

import os, pandas as pd, geopandas as gpd
from scipy.spatial import cKDTree
import numpy as np

RAW_SEM = "data/raw/semaforos/semaforos_raw.parquet"
CLEAN_DIR = "data/clean"
OUT_COMP = f"{CLEAN_DIR}/comparendos_2018_dist_semaforos.parquet"
OUT_SIN = f"{CLEAN_DIR}/siniestralidad_2018_dist_semaforos.parquet"


def _nearest_dist_m(points_gdf, sem_gdf):
    p_3857 = points_gdf.to_crs(3857)
    s_3857 = sem_gdf.to_crs(3857)
    tree = cKDTree(np.c_[s_3857.geometry.x, s_3857.geometry.y])
    dist, _ = tree.query(np.c_[p_3857.geometry.x, p_3857.geometry.y], k=1)
    return dist  # metros en 3857


def _bucket(d):
    if pd.isna(d):
        return "SIN_COORD"
    if d <= 100:
        return "0-100m"
    if d <= 300:
        return "100-300m"
    return ">300m"


def _to_points(df):
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs=4326
    )
    return gdf.dropna(subset=["geometry"])


def main():
    sem = pd.read_parquet(RAW_SEM)
    gsem = gpd.GeoDataFrame(
        sem, geometry=gpd.points_from_xy(sem["lon"], sem["lat"]), crs=4326
    ).dropna(subset=["geometry"])

    comp = pd.read_parquet(f"{CLEAN_DIR}/comparendos_2018_loc.parquet").dropna(
        subset=["lat", "lon"]
    )
    sin = pd.read_parquet(f"{CLEAN_DIR}/siniestralidad_2018_loc.parquet").dropna(
        subset=["lat", "lon"]
    )

    gcomp = _to_points(comp)
    gsin = _to_points(sin)

    comp["dist_sem_m"] = _nearest_dist_m(gcomp, gsem)
    sin["dist_sem_m"] = _nearest_dist_m(gsin, gsem)

    comp["dist_bucket"] = comp["dist_sem_m"].map(_bucket)
    sin["dist_bucket"] = sin["dist_sem_m"].map(_bucket)

    comp.to_parquet(OUT_COMP, index=False)
    sin.to_parquet(OUT_SIN, index=False)
    print(f"OK → {OUT_COMP} | {OUT_SIN}")


if __name__ == "__main__":
    main()
