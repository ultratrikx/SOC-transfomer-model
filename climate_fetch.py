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
import csv

ee.Authenticate()
ee.Initialize(project='carbon-seq-model')
print(ee.String('Hello from the Earth Engine servers!').getInfo())

def process_and_save_coordinate(coord, date_range, output_dir):
    print(f"\nProcessing coordinate: lat={coord['lat']}, lon={coord['lon']}, id={coord['id']}")
    results = []
    for date in date_range:
        start_str = date.strftime('%Y-%m-%d')
        end_str = (date + pd.offsets.MonthEnd(1)).strftime('%Y-%m-%d')
        try:
            point = ee.Geometry.Point([coord['lon'], coord['lat']])
            collection = (ee.ImageCollection('IDAHO_EPSCOR/TERRACLIMATE')
                          .select(['tmmn','tmmx','vpd','pr','srad',
                                   'aet','pdsi','def','pet','vap','soil'])
                          .filterDate(start_str, end_str))
            
            # Debug: check collection size
            size = collection.size().getInfo()
            if size == 0:
                print(f"Warning: No images found for {start_str} to {end_str}")
                continue
                
            region = point.buffer(100)
            mean_img = collection.mean().reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=region,
                scale=1000,
                maxPixels=1e9
            )

            data_dict = mean_img.getInfo()
            if not any(data_dict.values()):
                print(f"Warning: All null values returned for {start_str}")
                continue
                
            data_dict.update({
                'date': start_str,
                'id': coord['id'],
                'lat': coord['lat'],
                'lon': coord['lon']
            })
            results.append(data_dict)
        except ee.ee_exception.EEException as e:
            print(f"EE Error for {coord['id']} at {start_str}: {str(e)}")
            continue

    if results:
        df = pd.DataFrame(results)
        output_path = os.path.join(output_dir, f"{coord['id']}.csv")
        df.to_csv(output_path, index=False)
        print(f"Successfully saved {len(results)} records for {coord['id']}")
        return coord['id'], len(results)
    return coord['id'], 0


def fetch_and_save_climate_data(csv_path, output_dir, max_workers=4):
    os.makedirs(output_dir, exist_ok=True)
    
    # Read coordinate CSV
    coords = []
    with open(csv_path, 'r') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            coords.append({
                'id': row['id'],
                'lat': float(row['latitude']),
                'lon': float(row['longitude'])
            })

    date_range = pd.date_range(start='2015-01-01', end='2019-12-31', freq='MS')

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_and_save_coordinate, coord, date_range, output_dir): coord['id']
            for coord in coords
        }

        for future in tqdm(as_completed(futures), total=len(coords), desc="Processing locations"):
            coord_id = futures[future]
            try:
                id_, count = future.result()
                if count > 0:
                    print(f"Saved {count} records for {id_}")
                else:
                    print(f"No data found for {id_}")
            except Exception as e:
                print(f"Error processing coordinate {coord_id}: {str(e)}")


if __name__ == '__main__':
    fetch_and_save_climate_data('./csv/peatlands.csv', './data/climate', max_workers=4)