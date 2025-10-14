# Uso: python -m src.transform.join_localidades
import os, geopandas as gpd, pandas as pd

CLEAN = "data/clean"
RAW = "data/raw"
os.makedirs(CLEAN, exist_ok=True)


def to_gdf_points(df, lat="lat", lon="lon"):
    df = df.dropna(subset=[lat, lon]).copy()
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs=4326)


def sjoin_localidad(points, polys):
    # Buenas prácticas: usar 'within' y left join
    return gpd.sjoin(
        points,
        polys[["LOCALIDAD", "geometry"]].rename(
            columns={"LOCALIDAD": "LOCALIDAD_POLY"}
        ),
        predicate="within",
        how="left",
    ).rename(columns={"LOCALIDAD_POLY": "LOCALIDAD_JOIN"})


def main():
    gdf_loc = gpd.read_file(f"{RAW}/localidades_4326.geojson").to_crs(4326)

    comp = pd.read_parquet(f"{RAW}/comparendos_2018.parquet")
    comp_g = to_gdf_points(comp, "lat", "lon")
    comp_g = sjoin_localidad(comp_g, gdf_loc)
    comp_g.drop(columns=["index_right"], errors="ignore").to_parquet(
        f"{CLEAN}/comparendos_2018_loc.parquet", index=False
    )

    sin = pd.read_parquet(f"{CLEAN}/siniestralidad_2018_geocoded.parquet")
    sin_g = to_gdf_points(sin, "lat", "lon")
    sin_g = sjoin_localidad(sin_g, gdf_loc)
    sin_g.drop(columns=["index_right"], errors="ignore").to_parquet(
        f"{CLEAN}/siniestralidad_2018_loc.parquet", index=False
    )

    print("OK joins → data/clean/*_loc.parquet")


if __name__ == "__main__":
    main()
