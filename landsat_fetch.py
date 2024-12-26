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
        self.years = range(2015, 2021)  # 2015-2020
        self.summer_months = ['06', '07', '08']  # June, July, August
        self.winter_months = ['01', '02', '12']  # December, January, February
        
    def get_ndvi(self, image):
        """Calculate NDVI from Landsat 8 bands"""
        nir = image.select('SR_B5')
        red = image.select('SR_B4')
        return nir.subtract(red).divide(nir.add(red)).rename('NDVI')
    
    def get_seasonal_collection(self, season, year, region):
        """Get filtered collection for a specific season and year"""
        if season == 'summer':
            start_date = f'{year}-06-01'
            end_date = f'{year}-08-31'
        else:  # winter
            # Include December of previous year
            winter_start = f'{year-1}-12-01'
            winter_end = f'{year}-02-28'
            
        collection = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
                     .filterBounds(region)
                     .filter(ee.Filter.lt('CLOUD_COVER', 20))
                     # Filter out snow/ice
                     .filter(ee.Filter.lt('SNOW_ICE_COVER_PERCENT', 10)))
        
        if season == 'summer':
            # Get summer collection and sort by NDVI (descending)
            summer_collection = collection.filterDate(start_date, end_date)
            with_ndvi = summer_collection.map(lambda img: img.addBands(self.get_ndvi(img)))
            return with_ndvi.sort('NDVI', False)  # Highest NDVI first
        else:
            # Get winter collection and sort by NDVI (ascending)
            winter_collection = collection.filterDate(winter_start, winter_end)
            with_ndvi = winter_collection.map(lambda img: img.addBands(self.get_ndvi(img)))
            return with_ndvi.sort('NDVI', True)  # Lowest NDVI first
    
    def create_region(self, lat, lon):
        """Create a square region centered on the point"""
        point = ee.Geometry.Point([lon, lat])
        return point.buffer(self.region_size/2, 1).bounds()

    def fetch_landsat_data(self, lat, lon, location_id):
        """Fetch summer and winter images for each year"""
        region = self.create_region(lat, lon)
        metadata = []
        
        for year in self.years:
            print(f"\nProcessing year {year} for location {location_id}")
            
            # Get summer image (highest vegetation)
            summer_collection = self.get_seasonal_collection('summer', year, region)
            summer_image = summer_collection.first()
            
            # Get winter image (lowest vegetation)
            winter_collection = self.get_seasonal_collection('winter', year, region)
            winter_image = winter_collection.first()
            
            # If no suitable winter image, use second-best summer image
            if winter_image is None:
                print(f"No suitable winter image for {year}, using alternative summer image")
                winter_image = summer_collection.sort('NDVI', True).first()
            
            # Process both seasonal images
            for season, image in [('summer', summer_image), ('winter', winter_image)]:
                if image is not None:
                    scene_id = image.get('LANDSAT_SCENE_ID').getInfo()
                    cloud_cover = image.get('CLOUD_COVER').getInfo()
                    ndvi = image.select('NDVI').reduceRegion(
                        reducer=ee.Reducer.mean(),
                        geometry=region,
                        scale=30
                    ).get('NDVI').getInfo()
                    
                    print(f"Processing {season} image for {year} (Cloud: {cloud_cover}%, NDVI: {ndvi:.2f})")
                    
                    # Download each band
                    sr_bands = ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
                    
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
                    
                    # Update metadata
                    metadata.append({
                        'location_id': location_id,
                        'latitude': lat,
                        'longitude': lon,
                        'scene_id': scene_id,
                        'season': season,
                        'year': year,
                        'cloud_cover': cloud_cover,
                        'ndvi': ndvi,
                        'timestamp': datetime.datetime.now().isoformat()
                    })
        
        # Update metadata CSV
        self.update_metadata(metadata)
        return metadata

    def update_metadata(self, metadata):
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