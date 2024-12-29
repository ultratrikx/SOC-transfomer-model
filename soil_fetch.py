import pandas as pd
import rasterio
from rasterio.windows import from_bounds
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load CSV file
csv_file = './data/csv/peatlands.csv'
df = pd.read_csv(csv_file)
logging.info(f'Loaded CSV file: {csv_file}')

# Load GeoTIFF file
geotiff_file = './data/SOC/2016_SOC-30.tif'
dataset = rasterio.open(geotiff_file)
logging.info(f'Loaded GeoTIFF file: {geotiff_file}')

# Load second GeoTIFF file
geotiff_file_2 = './data/SOC/2016_SOC-1.tif'
dataset_2 = rasterio.open(geotiff_file_2)
logging.info(f'Loaded second GeoTIFF file: {geotiff_file_2}')

# Define the size of the patch (750m x 750m)
patch_size = 16000  # in meters

# Function to convert meters to degrees (approximation)
def meters_to_degrees(meters):
    return meters / 111320  # Approximate conversion factor

# Loop through each row in the CSV
bounding_boxes = []
for index, row in df.iterrows():
    unique_id = row['id']
    lon = row['longitude']
    lat = row['latitude']
    
    logging.info(f'Processing unique_id: {unique_id}, lon: {lon}, lat: {lat}')
    
    # Calculate the bounding box
    half_patch_size = meters_to_degrees(patch_size / 2)
    min_lon = lon - half_patch_size
    max_lon = lon + half_patch_size
    min_lat = lat - half_patch_size
    max_lat = lat + half_patch_size
    
    logging.debug(f'Bounding box - min_lon: {min_lon}, max_lon: {max_lon}, min_lat: {min_lat}, max_lat: {max_lat}')
    
    # Create a window to read the data
    window = from_bounds(min_lon, min_lat, max_lon, max_lat, dataset.transform)
    window_2 = from_bounds(min_lon, min_lat, max_lon, max_lat, dataset_2.transform)
    
    # Read the data from the window
    patch = dataset.read(window=window)
    patch_2 = dataset_2.read(window=window_2)
    
    # Define the output file paths
    output_file = f'./data/SOC/2016_30cm/{unique_id}.tif'
    output_file_2 = f'./data/SOC/2016_1m/{unique_id}.tif'
    
    # Save the patches as new GeoTIFF files
    with rasterio.open(
        output_file,
        'w',
        driver='GTiff',
        height=patch.shape[1],
        width=patch.shape[2],
        count=dataset.count,
        dtype=patch.dtype,
        crs='EPSG:4326',  # WGS84 CRS
        transform=rasterio.windows.transform(window, dataset.transform),
    ) as dst:
        dst.write(patch)
    logging.info(f'Saved patch to: {output_file}')
    
    with rasterio.open(
        output_file_2,
        'w',
        driver='GTiff',
        height=patch_2.shape[1],
        width=patch_2.shape[2],
        count=dataset_2.count,
        dtype=patch_2.dtype,
        crs='EPSG:4326',  # WGS84 CRS
        transform=rasterio.windows.transform(window_2, dataset_2.transform),
    ) as dst:
        dst.write(patch_2)
    logging.info(f'Saved patch to: {output_file_2}')
    
    # Append bounding box to the list
    bounding_boxes.append({
        'id': unique_id,
        'min_lon': min_lon,
        'min_lat': min_lat,
        'max_lon': max_lon,
        'max_lat': max_lat
    })

# Close the datasets
dataset.close()
dataset_2.close()
logging.info('Closed GeoTIFF datasets')

# Add bounding boxes to the DataFrame and save to CSV
bounding_boxes_df = pd.DataFrame(bounding_boxes)
df = df.merge(bounding_boxes_df, on='id')
output_csv = './data/csv/master.csv'
df.to_csv(output_csv, index=False)
logging.info(f'Saved updated CSV to: {output_csv}')
