import os
import numpy as np
import rasterio
from rasterio.warp import reproject, Resampling
from pathlib import Path
import pandas as pd

class DataProcessor:
    def __init__(self, soilgrids_dir, landsat_dir, output_dir, locations_csv):
        self.soilgrids_dir = soilgrids_dir
        self.landsat_dir = landsat_dir
        self.output_dir = output_dir
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        # Load location mappings
        self.locations_df = pd.read_csv(locations_csv)
        # Create dictionary mapping landsat_scene_id to location_id
        self.location_map = dict(zip(
            self.locations_df['landsat_scene_id'],
            self.locations_df['location_id']
        ))
    
    def resample_landsat(self, landsat_image, soilgrids_image, output_path):
        """Resample Landsat image to match the resolution and extent of SoilGrids image"""
        try:
            # Open both datasets simultaneously
            with rasterio.open(soilgrids_image) as soil_ds, rasterio.open(landsat_image) as landsat_ds:
                # Get SoilGrids metadata
                soil_transform = soil_ds.transform
                soil_crs = soil_ds.crs
                soil_width = soil_ds.width
                soil_height = soil_ds.height
                
                # Get the profile from SoilGrids and update it
                output_profile = soil_ds.profile.copy()
                
                # Read and resample Landsat data
                landsat_data = landsat_ds.read()
                landsat_resampled = np.empty(
                    shape=(landsat_data.shape[0], soil_height, soil_width), 
                    dtype=landsat_data.dtype
                )
                
                # Reproject and resample each band
                for i in range(landsat_data.shape[0]):
                    reproject(
                        source=landsat_data[i],
                        destination=landsat_resampled[i],
                        src_transform=landsat_ds.transform,
                        src_crs=landsat_ds.crs,
                        dst_transform=soil_transform,
                        dst_crs=soil_crs,
                        resampling=Resampling.average
                    )

                # Update profile for output
                output_profile.update(
                    dtype=landsat_data.dtype,
                    count=landsat_data.shape[0],
                    compress='lzw',       # Add compression
                    nodata=0              # Set nodata to a valid value for uint16
                )

                # Write resampled data
                with rasterio.open(output_path, 'w', **output_profile) as dst:
                    dst.write(landsat_resampled)

            # Delete original Landsat image after successful resampling
            os.remove(landsat_image)
            print(f"Deleted original file: {landsat_image}")
            return True
        except Exception as e:
            print(f"Error during resampling: {str(e)}")
            return False

    def process_all_images(self):
        """Process all Landsat images to match SoilGrids images"""
        print(f"Looking for images in: {self.landsat_dir}")
        if not os.path.exists(self.landsat_dir):
            print(f"ERROR: Landsat directory does not exist: {self.landsat_dir}")
            return
            
        found_files = False
        for root, dirs, files in os.walk(self.landsat_dir):
            print(f"Checking directory: {root}")
            
            for file in files:
                if file.upper().endswith('.TIF'):
                    found_files = True
                    landsat_image = os.path.join(root, file)
                    
                    # Extract Landsat scene ID from the directory name
                    scene_dir = os.path.basename(os.path.dirname(landsat_image))
                    
                    # Look up the corresponding location_id
                    location_id = self.location_map.get(scene_dir)
                    
                    if location_id:
                        soil_dir = os.path.join(self.soilgrids_dir, "tifs", f"location_{location_id}")
                        soilgrids_image = os.path.join(soil_dir, "soc_0-5cm_mean.tif")
                        
                        print(f"\nProcessing:")
                        print(f"Landsat scene: {scene_dir}")
                        print(f"Location ID: {location_id}")
                        print(f"Landsat image: {landsat_image}")
                        print(f"Looking for SoilGrids image: {soilgrids_image}")
                        
                        if os.path.exists(soilgrids_image):
                            output_path = os.path.join(self.output_dir, f"resampled_{location_id}_{file}")
                            print(f"Resampling to: {output_path}")
                            self.resample_landsat(landsat_image, soilgrids_image, output_path)
                            print(f"Successfully resampled {landsat_image}")
                        else:
                            print(f"ERROR: SoilGrids image not found: {soilgrids_image}")
                    else:
                        print(f"WARNING: No location ID mapping found for scene: {scene_dir}")
        
        if not found_files:
            print("No .TIF files were found in the Landsat directory!")

def main():
    base_dir = "c:/Users/Rohan/carbon-seq-model/processed_data"
    locations_csv = os.path.join(base_dir, "master_locations.csv")
    
    processor = DataProcessor(
        soilgrids_dir=os.path.join(base_dir, "soilgrids_data"),
        landsat_dir=os.path.join(base_dir, "landsat_data"),
        output_dir=os.path.join(base_dir, "resampled_landsat_data"),
        locations_csv=locations_csv
    )
    processor.process_all_images()

if __name__ == "__main__":
    main()
