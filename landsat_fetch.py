import json
import requests
import sys
import time
import datetime
import os
import pandas as pd
import warnings
import backoff
import urllib3
import tarfile
import ee
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_manager import DataManager



ee.Authenticate()
ee.Initialize(project='carbon-seq-model')
print(ee.String('Hello from the Earth Engine servers!').getInfo())

# warnings.filterwarnings("ignore")

class GEELandsatFetcher:
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.output_dir = self.data_manager.landsat_dir
        self.csv_path = self.data_manager.get_csv_path("north_american_forests.csv")
        self.pixel_size = 128
        self.required_columns = ['latitude', 'longitude']
        self.scale = 30  # Landsat resolution in meters
        self.region_size = 3840  # 128 pixels * 30m = 3840m
        
    def get_landsat_collection(self, start_date, end_date):
        return (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
                .filterDate(start_date, end_date)
                .filter(ee.Filter.lt('CLOUD_COVER', 20)))

    def create_region(self, lat, lon):
        """Create a square region centered on the point"""
        point = ee.Geometry.Point([lon, lat])
        return point.buffer(self.region_size/2, 1).bounds()

    def fetch_landsat_data(self, lat, lon, location_id, start_date='2024-01-01', end_date='2024-12-31'):
        region = self.create_region(lat, lon)
        collection = self.get_landsat_collection(start_date, end_date)
        
        # Get the least cloudy image
        image = collection.sort('CLOUD_COVER').first()
        if image is None:
            print(f"No images found for location {location_id}")
            return None

        # Get surface reflectance bands
        sr_bands = ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
        
        # Create unique identifier
        scene_id = image.get('LANDSAT_SCENE_ID').getInfo()
        
        # Download each band
        for band in sr_bands:
            output_path = self.data_manager.get_landsat_path(location_id, scene_id, band)
            
            if not os.path.exists(output_path):
                # Select and clip the band
                band_image = image.select(band).clip(region)
                
                # Get the download URL with correct parameters
                url = band_image.getDownloadURL({
                    'region': region,
                    'scale': self.scale,
                    'format': 'GEO_TIFF',
                })
                
                # Download the file
                response = requests.get(url)
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded {band} for location {location_id}")
        
        # Update metadata CSV
        self.update_metadata(location_id, lat, lon, scene_id)
        return scene_id

    def update_metadata(self, location_id, lat, lon, scene_id):
        metadata = {
            'location_id': [location_id],
            'latitude': [lat],
            'longitude': [lon],
            'scene_id': [scene_id],
            'timestamp': [datetime.datetime.now().isoformat()]
        }
        
        df = pd.DataFrame(metadata)
        if os.path.exists(self.csv_path):
            existing_df = pd.read_csv(self.csv_path)
            df = pd.concat([existing_df, df], ignore_index=True)
        
        df.to_csv(self.csv_path, index=False)

    def process_coordinates(self, coordinates_df):
        """Process coordinates with automatic location ID generation if needed"""
        # Validate required columns
        if not all(col in coordinates_df.columns for col in self.required_columns):
            raise ValueError(f"CSV must contain columns: {self.required_columns}")
        
        # Add location_id if not present
        if 'location_id' not in coordinates_df.columns:
            coordinates_df['location_id'] = [f'loc_{i:04d}' for i in range(len(coordinates_df))]
            
            # Save the updated DataFrame with location IDs
            coordinates_df.to_csv(self.csv_path, index=False)
            print(f"Added location IDs and saved to {self.csv_path}")

        for _, row in coordinates_df.iterrows():
            try:
                self.fetch_landsat_data(
                    float(row['latitude']),
                    float(row['longitude']),
                    str(row['location_id'])
                )
            except Exception as e:
                print(f"Error processing location {row['location_id']}: {str(e)}")
                continue


# Initialize DataManager before GEELandsatFetcher
data_manager = DataManager(base_dir="data")
fetcher = GEELandsatFetcher(data_manager)

# Load and validate coordinates
try:
    coordinates_df = pd.read_csv(data_manager.get_csv_path("north_american_forests.csv"))
    print(f"Loaded {len(coordinates_df)} coordinates")
    fetcher.process_coordinates(coordinates_df)
except Exception as e:
    print(f"Error: {str(e)}")