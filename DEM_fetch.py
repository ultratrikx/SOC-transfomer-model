import json
import requests
import sys
import time
import datetime
import os
import pandas as pd
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
from rasterio.warp import transform_bounds, transform_geom

# Set up logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"elevation_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

ee.Authenticate()
ee.Initialize(project='carbon-seq-model')
print(ee.String('Hello from the Earth Engine servers!').getInfo())


SOC_files = "./data/SOC/2016_1m"

def get_dem_for_geotiff(geotiff_path):
    """Fetch DEM data matching the extent and resolution of input geotiff."""
    logger.info(f"Processing file: {geotiff_path}")
    
    try:
        with rasterio.open(geotiff_path) as src:
            bounds = src.bounds
            transform = src.transform
            crs = src.crs.to_string()
            width = src.width
            height = src.height
            
            # Transform bounds to EPSG:4326
            bounds_4326 = transform_bounds(src.crs, 'EPSG:4326', 
                                        bounds.left, bounds.bottom, 
                                        bounds.right, bounds.top)
            
            # Create Earth Engine geometry
            roi = ee.Geometry.Rectangle(
                coords=[
                    bounds_4326[0], bounds_4326[1],
                    bounds_4326[2], bounds_4326[3]
                ],
                proj='EPSG:4326'
            )
        
        # Get SRTM elevation data
        dem = ee.Image('USGS/SRTMGL1_003').select('elevation')
        
        try:
            # Reproject and resample DEM at 250m resolution
            dem_resampled = dem.resample('bilinear').reproject(
                crs=crs,
                scale=250
            ).clip(roi)

            # Sample rectangle without scale parameter
            result = dem_resampled.sampleRectangle(
                region=roi,
                defaultValue=-9999
            ).getInfo()

            # Extract elevation values and ensure 64x64 shape
            dem_array = np.array(result['properties']['elevation'])
            if dem_array.shape != (64, 64):
                logger.warning(f"Reshaping array from {dem_array.shape} to (64, 64)")
                dem_array = np.resize(dem_array, (64, 64))
            
            # Save as GeoTIFF
            # output_path = Path(geotiff_path).parent / f"{Path(geotiff_path).stem}_dem.tif"
            output_path = Path('./data/DEM') / f"{Path(geotiff_path).stem}_dem.tif"
            os.makedirs('./data/DEM', exist_ok=True)
            
            with rasterio.open(
                output_path,
                'w',
                driver='GTiff',
                height=64,
                width=64,
                count=1,
                dtype=rasterio.float32,
                crs=crs,
                transform=transform,
            ) as dst:   
                dst.write(dem_array.astype(rasterio.float32), 1)
            
            logger.info(f"Successfully saved DEM to: {output_path}")
            return output_path
            
        except EEException as e:
            logger.error(f"Earth Engine error for {geotiff_path}: {str(e)}")
            return None
            
    except Exception as e:
        logger.error(f"Unexpected error processing {geotiff_path}: {str(e)}", exc_info=True)
        return None

def process_all_geotiffs(input_folder):
    """Process all GeoTIFF files in the input folder."""
    logger.info(f"Starting batch processing in folder: {input_folder}")
    geotiff_files = list(Path(input_folder).glob('*.tif'))
    logger.info(f"Found {len(geotiff_files)} files to process")
    
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(get_dem_for_geotiff, str(f)) for f in geotiff_files]
        
        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            if result:
                successful += 1
            else:
                failed += 1
    
    logger.info(f"Processing complete. Successful: {successful}, Failed: {failed}")

if __name__ == "__main__":
    logger.info("Starting DEM extraction process")
    try:
        process_all_geotiffs(SOC_files)
    except Exception as e:
        logger.error("Fatal error in main process", exc_info=True)
    logger.info("Process completed")
