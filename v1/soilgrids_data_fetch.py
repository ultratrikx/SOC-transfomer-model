import pandas as pd
import numpy as np
from soilgrids import SoilGrids
import matplotlib.pyplot as plt 
from pathlib import Path
import rasterio
from pyproj import Transformer
import requests
from tqdm import tqdm
import os

class SoilGridsFetcher:
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.output_dir = str(data_manager.soilgrids_dir)
        self.soil_grids = SoilGrids()
        self.buffer_size = 10000  # meters instead of degrees
        
        # Create output directories
        self.data_dir = os.path.join(self.output_dir, "tifs")
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        
        # Only SOC and OCD variables at 0-5cm depth
        self.variables = {
            "soc": ["0-5cm"],    # Soil organic carbon content
            "ocd": ["0-5cm"]     # Organic carbon density
        }
        
        # Create lookup for full variable names
        self.variable_names = {
            "soc": "Soil organic carbon content",
            "ocd": "Organic carbon density" 
        }

    def convert_to_isric_crs(self, lat, lon):
        """Convert lat/lon to ISRIC CRS coordinates"""
        # Replace the target CRS with EPSG:3857 (Web Mercator) which uses meters
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857")
        x, y = transformer.transform(lat, lon)
        return x, y

    def get_location_data(self, lat, lon, location_id):
        """Download SOC/OCD data and update data manager"""
        with requests.Session() as session:
            try:
                print(f"Processing location {location_id} ({lat, lon})")
                
                # Convert coordinates to ISRIC projection 
                x, y = self.convert_to_isric_crs(lat, lon)
                
                # Calculate bounding box
                west = x - self.buffer_size
                east = x + self.buffer_size
                south = y - self.buffer_size  
                north = y + self.buffer_size

                # Calculate width and height based on buffer size and resolution
                resolution = 30  # meters/pixel
                width = int((east - west) / resolution)
                height = int((north - south) / resolution)

                # Create location directory
                location_dir = os.path.join(self.data_dir, f"location_{location_id}")
                Path(location_dir).mkdir(exist_ok=True)

                download_success = True  # Track if all downloads are successful

                # Download each carbon variable
                for var_name, depths in self.variables.items():
                    for depth in depths:
                        try:
                            coverage_id = f"{var_name}_{depth}_mean"
                            output_file = os.path.join(location_dir, f"{coverage_id}.tif")

                            print(f"Downloading {self.variable_names[var_name]} ({depth})...")
                            
                            data = self.soil_grids.get_coverage_data(
                                service_id=var_name,
                                coverage_id=coverage_id,
                                west=west,
                                south=south,
                                east=east,  
                                north=north,
                                crs="urn:ogc:def:crs:EPSG::3857",
                                width=width,
                                height=height,
                                output=output_file
                            )

                            # Check if file was created and has content
                            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                                meta_file = os.path.join(location_dir, f"{coverage_id}_metadata.txt")
                                with open(meta_file, 'w') as f:
                                    f.write(f"Variable: {self.variable_names[var_name]}\n")
                                    f.write(f"Depth: {depth}\n")
                                    for key, value in self.soil_grids.metadata.items():
                                        f.write(f"{key}: {value}\n")
                                print(f"Successfully downloaded: {coverage_id}")
                            else:
                                print(f"Download failed or empty file: {coverage_id}")
                                download_success = False
                                break

                        except Exception as e:
                            print(f"Error downloading {var_name} {depth}: {str(e)}")
                            download_success = False
                            continue

                # Only update data manager if all downloads were successful
                if download_success:
                    self.data_manager.update_soilgrids_id(location_id, str(location_id))
                else:
                    # Clean up failed downloads
                    if os.path.exists(location_dir):
                        for file in os.listdir(location_dir):
                            os.remove(os.path.join(location_dir, file))
                        os.rmdir(location_dir)
                    print(f"Failed to download all data for location {location_id}")
                
            except Exception as e:
                print(f"Error downloading data for location {location_id}: {str(e)}")

    def process_coordinates(self, csv_file):
        """Process all coordinates from CSV file"""
        df = pd.read_csv(csv_file)
        
        print(f"Processing {len(df)} locations...")
        
        for idx, row in df.iterrows():
            try:
                self.get_location_data(
                    row['latitude'], 
                    row['longitude'],
                    idx
                )
            except Exception as e:
                print(f"Error processing location {idx}: {str(e)}")

def main():
    fetcher = SoilGridsFetcher()
    fetcher.process_coordinates("/csv/north_american_forests.csv")

if __name__ == "__main__":
    main()
