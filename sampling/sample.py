import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box
import numpy as np
import os

# Step 1: Load Shapefile
shapefile_path = './canada_peats/Canada_Peatland.shp'

# Set CRS explicitly to WGS84
crs = 'EPSG:4326'
peatlands = gpd.read_file(shapefile_path).to_crs(crs)

# Step 2: Filter Peatlands by Threshold
threshold = 40   # Example threshold for peatland percentage
filtered_peatlands = peatlands[peatlands['PEAT_PER'] > threshold]

#Output the number of identified features
num_features = len(filtered_peatlands)
print(f"Number of identified features above threshold ({threshold}%): {num_features}")

# Step 3: Create a 750m x 750m Grid
minx, miny, maxx, maxy = filtered_peatlands.total_bounds
x_coords = np.arange(minx, maxx, 0.01)  # Approximate grid size in degrees
y_coords = np.arange(miny, maxy, 0.01)
grid_cells = []

for x in x_coords:
    for y in y_coords:
        grid_cells.append(box(x, y, x + 0.01, y + 0.01))

grid = gpd.GeoDataFrame({'geometry': grid_cells}, crs=filtered_peatlands.crs)

# Step 4: Clip Grid to Filtered Peatlands
training_data = gpd.overlay(grid, filtered_peatlands, how='intersection')

# Step 5: Export Separate GeoTIFF for Each Feature
output_folder = 'peatland_tiffs'
os.makedirs(output_folder, exist_ok=True)
resolution = 0.01

csv_rows = []

for index, cell in training_data.iterrows():
    feature_tiff = os.path.join(output_folder, f'peatland_feature_{index}.tif')
    minx, miny, maxx, maxy = cell.geometry.bounds
    transform = from_origin(minx, maxy, resolution, resolution)
    width = int((maxx - minx) / resolution)
    height = int((maxy - miny) / resolution)
    
    with rasterio.open(
        feature_tiff,
        'w',
        driver='GTiff',
        height=height,
        width=width,
        count=1,
        dtype='float32',
        crs=filtered_peatlands.crs,
        transform=transform
    ) as dst:
        raster_data = np.ones((height, width), dtype='float32')
        dst.write(raster_data, 1)
    
    csv_rows.append({
        'feature_id': index,
        'minx': minx,
        'miny': miny,
        'maxx': maxx,
        'maxy': maxy,
        'tiff_file': feature_tiff
    })

# Step 6: Export CSV
csv_output = 'peatland_training_data.csv'
csv_df = pd.DataFrame(csv_rows)
csv_df.to_csv(csv_output, index=False)

print(f"GeoTIFF files saved in '{output_folder}'")
print(f"CSV saved as '{csv_output}'")
