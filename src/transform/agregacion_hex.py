# Uso: python -m src.transform.agregacion_hex
# Genera grid 500m y cuenta comparendos + siniestros
import os, numpy as np, pandas as pd, geopandas as gpd

CLEAN, ANAL = "data/clean", "data/analytics"
os.makedirs(ANAL, exist_ok=True)


def grid_500m(gdf):
    gdf = gdf.to_crs(3857)
    xmin, ymin, xmax, ymax = gdf.total_bounds
    size = 500
    xs = np.arange(xmin, xmax + size, size)
    ys = np.arange(ymin, ymax + size, size)
    polys = [gpd.box(x, y, x + size, y + size) for x in xs[:-1] for y in ys[:-1]]
    return gpd.GeoDataFrame(geometry=polys, crs=3857).to_crs(4326)


def count_in_grid(points, grid, col_name):
    join = gpd.sjoin(points, grid, predicate="within")
    counts = join.groupby(join.index_right).size().rename(col_name)
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
    grid = count_in_grid(comp_g, grid, "comparendos")
    grid = count_in_grid(sin_g, grid, "siniestros")

    grid["score"] = grid["comparendos"] + grid["siniestros"]
    grid.to_file(f"{ANAL}/grid_hotspots.geojson", driver="GeoJSON")
    print("OK grid_hotspots.geojson", len(grid))


if __name__ == "__main__":
    main()
