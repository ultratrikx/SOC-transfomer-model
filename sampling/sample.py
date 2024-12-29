import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import numpy as np

# Step 1: Load Shapefile
shapefile_path = './canada_peats/Canada_Peatland.shp'
peatlands = gpd.read_file(shapefile_path)

# Ensure CRS is in meters for accurate distance calculations
peatlands = peatlands.to_crs(epsg=3347)  # Example CRS: NAD83 / Canada LCC (Adjust if needed)
print(f"Loaded {len(peatlands)} peatland polygons from '{shapefile_path}'")
print(f"CRS: {peatlands.crs}")
print(f"Bounds: {peatlands.total_bounds}")
print(f"Columns: {peatlands.columns}")
print(f"Sample Data: {peatlands.head()}")
print(f"Sample Geometry: {peatlands.geometry.head()}")
print(f"Sample Area: {peatlands.area.head()}")
print(f"Sample Perimeter: {peatlands.length.head()}")
print(f"Sample Centroid: {peatlands.centroid.head()}")
print(f"Sample Convex Hull: {peatlands.convex_hull.head()}")
print(f"Sample Envelope: {peatlands.envelope.head()}")
print(f"Sample Boundary: {peatlands.boundary.head()}")
print(f"Sample Buffer: {peatlands.buffer(1000).head()}")

# Step 2: Create a Grid of Points ~750m Apart
minx, miny, maxx, maxy = peatlands.total_bounds
x_coords = np.arange(minx, maxx, 25000)
y_coords = np.arange(miny, maxy, 25000)
print(f"Creating grid of {len(x_coords) * len(y_coords)} points...")
print(f"X Coordinates: {x_coords[:5]}...{x_coords[-5:]}")
print(f"Y Coordinates: {y_coords[:5]}...{y_coords[-5:]}")


# Generate Points on the Grid
points = [Point(x, y) for x in x_coords for y in y_coords]
print(f"Generated {len(points)} points")
grid_points = gpd.GeoDataFrame(geometry=points, crs=peatlands.crs)
print(f"Sample Grid Points: {grid_points.head()}")

# Step 3: Clip Points to Peatland Area
spatially_diverse_points = gpd.sjoin(grid_points, peatlands, how='inner', predicate='within')

# Step 4: Save Points to CSV
spatially_diverse_points.to_crs(epsg=4326)  # Convert back to Lat/Lon for CSV export
spatially_diverse_points['lon'] = spatially_diverse_points.geometry.x
spatially_diverse_points['lat'] = spatially_diverse_points.geometry.y

spatially_diverse_points[['lon', 'lat']].to_csv('spatially_diverse_points_25000.csv', index=False)

print(f"{len(spatially_diverse_points)} spatially diverse points saved to 'spatially_diverse_points.csv'")
