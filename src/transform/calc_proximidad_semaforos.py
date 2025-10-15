# Uso: python -m src.transform.calc_proximidad_semaforos
import os, geopandas as gpd, pandas as pd

CLEAN = "data/clean"
RAW = "data/raw"
os.makedirs(CLEAN, exist_ok=True)


def main():
    sin = pd.read_parquet(f"{CLEAN}/siniestralidad_2018_loc.parquet")
    sem = pd.read_parquet(f"{RAW}/semaforos.parquet")
    sin_g = gpd.GeoDataFrame(
        sin, geometry=gpd.points_from_xy(sin["lon"], sin["lat"]), crs=4326
    ).to_crs(3857)
    sem_g = gpd.GeoDataFrame(
        sem, geometry=gpd.points_from_xy(sem["lon"], sem["lat"]), crs=4326
    ).to_crs(3857)

    # Buenas prácticas: sjoin_nearest (distancia en metros en EPSG:3857)
    joined = gpd.sjoin_nearest(
        sin_g, sem_g[["geometry"]], how="left", distance_col="dist_m"
    )
    bins = [0, 100, 300, 1e9]
    labels = ["<100 m", "100–300 m", ">300 m"]
    joined["dist_bucket"] = pd.cut(
        joined["dist_m"], bins=bins, labels=labels, right=True
    )
    joined.to_parquet(
        f"{CLEAN}/siniestralidad_2018_proximidad_semaforos.parquet", index=False
    )
    print("OK → data/clean/siniestralidad_2018_proximidad_semaforos.parquet")


if __name__ == "__main__":
    main()
