import pandas as pd
import numpy as np
from soilgrids import SoilGrids
import matplotlib.pyplot as plt 
import os
from pathlib import Path
import rasterio
from pyproj import Transformer
import requests

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
        # Create a session for this download
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

                # Define resolution in degrees per pixel
                resolution = 100  # meters/pixel

                # Calculate width and height based on buffer size and resolution
                width = int((east - west) / resolution)
                height = int((north - south) / resolution)

                # Create location directory
                location_dir = os.path.join(self.data_dir, f"location_{location_id}")
                Path(location_dir).mkdir(exist_ok=True)

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
                                crs="urn:ogc:def:crs:EPSG::3857",  # Updated CRS
                                width=width,    # Added width
                                height=height,  # Added height
                                output=output_file
                            )

                            # Save metadata
                            meta_file = os.path.join(location_dir, f"{coverage_id}_metadata.txt")
                            with open(meta_file, 'w') as f:
                                f.write(f"Variable: {self.variable_names[var_name]}\n")
                                f.write(f"Depth: {depth}\n")
                                for key, value in self.soil_grids.metadata.items():
                                    f.write(f"{key}: {value}\n")

                        except Exception as e:
                            print(f"Error downloading {var_name} {depth}: {str(e)}")

                # Update data manager after successful download
                self.data_manager.update_soilgrids_id(location_id, str(location_id))
                
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
    fetcher.process_coordinates("north_american_forests.csv")

if __name__ == "__main__":
    main()
