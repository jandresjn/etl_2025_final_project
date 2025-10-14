# Uso: python -m src.transform.agregacion_hotspots
import os, numpy as np, geopandas as gpd, pandas as pd

CLEAN, ANAL = "data/clean", "data/analytics"
os.makedirs(ANAL, exist_ok=True)


def grid_500m(gdf_4326):
    # Buenas prácticas: generar grid en métrico (EPSG:3857) y regresar a 4326
    gdf = gdf_4326.to_crs(3857)
    xmin, ymin, xmax, ymax = gdf.total_bounds
    size = 500
    xs = np.arange(xmin, xmax + size, size)
    ys = np.arange(ymin, ymax + size, size)
    cells = []
    for x in xs[:-1]:
        for y in ys[:-1]:
            cells.append(((x, y), (x + size, y + size)))
    polys = gpd.GeoSeries(
        [gpd.box(x0, y0, x1, y1) for (x0, y0), (x1, y1) in cells], crs=3857
    )
    return gpd.GeoDataFrame(geometry=polys).to_crs(4326)


def count_in_grid(points_gdf, grid_gdf, col_name):
    join = gpd.sjoin(points_gdf[["geometry"]], grid_gdf, predicate="within")
    counts = join.groupby(join.index_right).size().rename(col_name)
    grid = grid_gdf.copy()
    grid[col_name] = counts
    grid[col_name] = grid[col_name].fillna(0).astype(int)
    return grid


def main():
    comp = pd.read_parquet(f"{CLEAN}/comparendos_2018_loc.parquet")
    sin = pd.read_parquet(f"{CLEAN}/siniestralidad_2018_loc.parquet")

    comp_g = gpd.GeoDataFrame(
        comp, geometry=gpd.points_from_xy(comp["lon"], comp["lat"]), crs=4326
    )
    sin_g = gpd.GeoDataFrame(
        sin, geometry=gpd.points_from_xy(sin["lon"], sin["lat"]), crs=4326
    )

    grid = grid_500m(pd.concat([comp_g, sin_g]))
    grid_comp = count_in_grid(comp_g, grid, "comparendos")
    grid_both = count_in_grid(sin_g, grid_comp, "siniestros")

    grid_both["score_hotspot"] = grid_both["comparendos"] + grid_both["siniestros"]
    grid_both.to_file(f"{ANAL}/grid_hotspots.geojson", driver="GeoJSON")
    print("OK → data/analytics/grid_hotspots.geojson")


if __name__ == "__main__":
    main()
