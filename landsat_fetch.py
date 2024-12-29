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
import rasterio
import numpy as np
from ee.ee_exception import EEException
import logging
import sys



ee.Authenticate()
ee.Initialize(project='carbon-seq-model')
print(ee.String('Hello from the Earth Engine servers!').getInfo())

# warnings.filterwarnings("ignore")

class GEELandsatFetcher:
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.output_dir = self.data_manager.landsat_dir
        self.scale = 30  # Landsat resolution in meters
        self.years = range(2015, 2019)  # 2015-2020
        self.max_cloud_cover = 30  # Increased from 20
        self.max_snow_cover = 20   # Increased from 10
        
        # Create seasonal directories
        self.summer_dir = os.path.join(self.output_dir, 'summer')
        self.winter_dir = os.path.join(self.output_dir, 'winter')
        os.makedirs(self.summer_dir, exist_ok=True)
        os.makedirs(self.winter_dir, exist_ok=True)
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        self.crs = None
        self.target_pixels = 64  # Target size in pixels
        self.target_size = 16000  # Target size in meters (16km)
        self.target_resolution = self.target_size / self.target_pixels  # 250m per pixel
        self.collection_timeout = 30  # seconds timeout for collection queries
        self.max_retries = 3
        self.retry_delay = 5

        self.logger = logging.getLogger("GEELandsatFetcher")
        self.logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        self.failed_files = []

    def get_ndvi(self, image):
        """Calculate NDVI from Landsat 8 bands"""
        nir = image.select('SR_B5')
        red = image.select('SR_B4')
        return nir.subtract(red).divide(nir.add(red)).rename('NDVI')
    
    def get_ndwi(self, image):
        """Calculate NDWI from Landsat 8 bands"""
        nir = image.select('SR_B5')
        green = image.select('SR_B3')
        return green.subtract(nir).divide(green.add(nir)).rename('NDWI')

    def get_geotiff_bounds(self, geotiff_path):
        """Get the bounds and CRS of a GeoTIFF file"""
        with rasterio.open(geotiff_path) as src:
            bounds = src.bounds
            # Store the CRS from the first GeoTIFF if not already set
            if not self.crs:
                self.crs = self.data_manager.get_reference_crs(geotiff_path)
            
            # Add padding to ensure complete coverage
            width = bounds.right - bounds.left
            height = bounds.top - bounds.bottom
            padding_x = width * 0.2
            padding_y = height * 0.2
            
            return {
                'left': bounds.left - padding_x,
                'right': bounds.right + padding_x,
                'top': bounds.top + padding_y,
                'bottom': bounds.bottom - padding_y
            }

    def create_region_from_geotiff(self, bounds):
        """Create a region from GeoTIFF bounds"""
        coords = [
            [bounds['left'], bounds['bottom']],
            [bounds['left'], bounds['top']],
            [bounds['right'], bounds['top']],
            [bounds['right'], bounds['bottom']],
            [bounds['left'], bounds['bottom']]
        ]
        return ee.Geometry.Polygon([coords])

    def get_collection_size_with_timeout(self, collection):
        """Get collection size with timeout"""
        try:
            size = collection.size().getInfo()
            return size if size is not None else 0
        except EEException as e:
            print(f"Earth Engine error: {str(e)}")
            return 0
        except Exception as e:
            print(f"Error getting collection size: {str(e)}")
            return 0

    def get_seasonal_collection(self, season, year, region):
        """Get filtered collection for a specific season and year"""
        try:
            if (season == 'summer'):
                start_date = f'{year}-06-01'
                end_date = f'{year}-09-30'
                dates = [(start_date, end_date)]
            else:
                dates = [
                    (f'{year-1}-12-01', f'{year}-02-28'),  # Winter
                    (f'{year}-03-01', f'{year}-05-31'),    # Spring
                ]
                
            # Initial collection with strict filters
            filtered_collection = None
            for start_date, end_date in dates:
                collection = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
                            .filterBounds(region)
                            .filterDate(start_date, end_date)
                            .filter(ee.Filter.lt('CLOUD_COVER', self.max_cloud_cover)))
                
                size = self.get_collection_size_with_timeout(collection)
                if size > 0:
                    if filtered_collection is None:
                        filtered_collection = collection
                    else:
                        filtered_collection = filtered_collection.merge(collection)

            # If no images found, try relaxed filters
            if filtered_collection is None or self.get_collection_size_with_timeout(filtered_collection) == 0:
                print(f"No {season} images found for {year}, trying with relaxed filters")
                filtered_collection = None
                for start_date, end_date in dates:
                    collection = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
                                .filterBounds(region)
                                .filterDate(start_date, end_date)
                                .filter(ee.Filter.lt('CLOUD_COVER', 50)))
                    
                    size = self.get_collection_size_with_timeout(collection)
                    if size > 0:
                        if filtered_collection is None:
                            filtered_collection = collection
                        else:
                            filtered_collection = filtered_collection.merge(collection)

            if filtered_collection is None or self.get_collection_size_with_timeout(filtered_collection) == 0:
                print(f"No images found for {season} {year}")
                return None

            size = self.get_collection_size_with_timeout(filtered_collection)
            print(f"Found {size} images for {season} {year}")
            
            with_ndvi = filtered_collection.map(lambda img: img.addBands(self.get_ndvi(img)))
            return with_ndvi.sort('NDVI', False if season == 'summer' else True)

        except Exception as e:
            print(f"Error processing {season} {year}: {str(e)}")
            return None

    def fetch_landsat_for_geotiff(self, geotiff_path):
        """Fetch Landsat data for a specific GeoTIFF region"""
        bounds = self.get_geotiff_bounds(geotiff_path)
        region = self.create_region_from_geotiff(bounds)
        image_id = os.path.splitext(os.path.basename(geotiff_path))[0]
        
        for year in self.years:
            print(f"\nProcessing year {year} for {image_id}")
            
            for season, output_dir in [('summer', self.summer_dir), ('winter', self.winter_dir)]:
                collection = self.get_seasonal_collection(season, year, region)
                if collection is None:
                    continue
                
                image = collection.first()
                if image is None:
                    print(f"No valid image found for {season} {year}")
                    continue

                try:
                    # Validate image before processing
                    band_names = image.bandNames().getInfo()
                    if not band_names:
                        print(f"Invalid image for {season} {year}: No bands available")
                        continue

                    # Create season-specific subfolder
                    season_year_dir = os.path.join(output_dir, f'{year}')
                    os.makedirs(season_year_dir, exist_ok=True)
                    
                    # Process bands with retry mechanism
                    for retry in range(self.max_retries):
                        try:
                            self._process_bands(image, image_id, season_year_dir, region)
                            break
                        except Exception as e:
                            if retry == self.max_retries - 1:
                                print(f"Failed to process {image_id} after {self.max_retries} attempts: {str(e)}")
                            else:
                                print(f"Retry {retry + 1}/{self.max_retries} for {image_id}")
                                time.sleep(self.retry_delay)
                                
                except Exception as e:
                    print(f"Error processing {image_id} for {season} {year}: {str(e)}")
                    continue

    def _process_bands(self, image, image_id, output_dir, region):
        """Process and download all bands for an image"""
        bands_to_download = {
            'SR_B1': 'B1', 'SR_B2': 'B2', 'SR_B3': 'B3',
            'SR_B4': 'B4', 'SR_B5': 'B5'
        }
        
        # Add indices
        image = image.addBands(self.get_ndvi(image))
        image = image.addBands(self.get_ndwi(image))
        bands_to_download.update({'NDVI': 'NDVI', 'NDWI': 'NDWI'})
        
        for band, band_name in bands_to_download.items():
            output_path = os.path.join(output_dir, f'{image_id}_{band_name}.tif')
            if not os.path.exists(output_path):
                band_image = image.select(band).clip(region)

                url = band_image.getDownloadURL({
                    'region': region,
                    'dimensions': [self.target_pixels, self.target_pixels],
                    'format': 'GEO_TIFF',
                    'crs': str(self.crs),
                })
                
                response = requests.get(url)
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                
                # Verify dimensions
                with rasterio.open(output_path) as src:
                    if src.shape != (self.target_pixels, self.target_pixels):
                        print(f"Warning: Dimension mismatch for {band_name}. "
                              f"Expected {(self.target_pixels, self.target_pixels)}, got {src.shape}")
                    if str(src.crs) != str(self.crs):
                        print(f"Warning: CRS mismatch for {band_name}. Expected {self.crs}, got {src.crs}")
                
                print(f"Downloaded {band_name} for {image_id}")

    def process_geotiffs(self, geotiff_dir):
        """Process all GeoTIFF files in a directory"""
        try:
            # Get CRS from first GeoTIFF
            first_tiff = next(f for f in os.listdir(geotiff_dir) if f.endswith('.tif'))
            first_tiff_path = os.path.join(geotiff_dir, first_tiff)
            self.crs = self.data_manager.get_reference_crs(first_tiff_path)
            print(f"Using CRS from reference GeoTIFF: {self.crs}")
            
            # Process all GeoTIFFs
            for file in os.listdir(geotiff_dir):
                if file.endswith('.tif'):
                    geotiff_path = os.path.join(geotiff_dir, file)
                    self.logger.info(f"Processing file: {file}")
                    try:
                        print(f"\nProcessing file: {file}")
                        self.fetch_landsat_for_geotiff(geotiff_path)
                    except Exception as e:
                        self.logger.error(f"Error processing {file}: {str(e)}")
                        self.failed_files.append(file)
                        print(f"Error processing {file}: {str(e)}")
                        print("Continuing with next file...")
                        continue
            self.logger.info(f"Failed files: {self.failed_files}")
                    
        except Exception as e:
            print(f"Fatal error in process_geotiffs: {str(e)}")
            raise

# Initialize and run
if __name__ == "__main__":
    data_manager = DataManager(base_dir="data")
    fetcher = GEELandsatFetcher(data_manager)
    
    # Specify your GeoTIFF directory
    geotiff_dir = "./data/SOC/2016_1m"
    fetcher.process_geotiffs(geotiff_dir)