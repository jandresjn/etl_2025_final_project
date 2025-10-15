# Uso: python -m src.transform.agregacion_hex
# Crea grilla ~500 m y cuenta comparendos/siniestros por celda. Exporta GeoJSON.

import os, numpy as np, pandas as pd, geopandas as gpd
from shapely.geometry import box

CLEAN = "data/clean"
ANAL = "data/analytics"
os.makedirs(ANAL, exist_ok=True)

OUT_GEO = f"{ANAL}/grid_hotspots.geojson"


def _to_points(df):
    return gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs=4326
    )


def _grid_500(gdf: gpd.GeoDataFrame, size_m=500) -> gpd.GeoDataFrame:
    if gdf.empty:
        raise ValueError("No hay geometrías para calcular el bounding box del grid.")
    g = gdf.to_crs(3857)
    xmin, ymin, xmax, ymax = g.total_bounds
    xs = np.arange(xmin, xmax + size_m, size_m)
    ys = np.arange(ymin, ymax + size_m, size_m)
    polys = [box(x, y, x + size_m, y + size_m) for x in xs[:-1] for y in ys[:-1]]
    grid = gpd.GeoDataFrame(geometry=polys, crs=3857).to_crs(4326)
    return grid


def _count(
    points: gpd.GeoDataFrame, grid: gpd.GeoDataFrame, name: str
) -> gpd.GeoDataFrame:
    # within es suficiente; si te interesan puntos en borde, usa predicate="intersects"
    join = gpd.sjoin(points, grid, predicate="within", how="left")
    counts = join.groupby(join.index_right).size()
    grid[name] = counts
    grid[name] = grid[name].fillna(0).astype(int)
    return grid


def main():
    comp = pd.read_parquet(f"{CLEAN}/comparendos_2018_loc.parquet").dropna(
        subset=["lat", "lon"]
    )
    sin = pd.read_parquet(f"{CLEAN}/siniestralidad_2018_loc.parquet").dropna(
        subset=["lat", "lon"]
    )

    if comp.empty and sin.empty:
        raise ValueError("No hay puntos para generar hotspots.")

    gcomp = _to_points(comp)
    gsin = _to_points(sin)

    # grid sobre el conjunto de puntos
    both = pd.concat([gcomp, gsin], ignore_index=True)
    grid = _grid_500(both, size_m=500)

    grid = _count(gcomp, grid, "comparendos")
    grid = _count(gsin, grid, "siniestros")
    grid["score"] = (grid["comparendos"] + grid["siniestros"]).astype(int)

    grid.to_file(OUT_GEO, driver="GeoJSON")
    print(f"OK → {OUT_GEO} ({len(grid)})")


if __name__ == "__main__":
    main()
