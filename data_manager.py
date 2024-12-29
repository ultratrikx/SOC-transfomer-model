import os
from pathlib import Path
import rasterio

class DataManager:
    def __init__(self, base_dir="data", target_crs=None):
        self.base_dir = Path(base_dir)
        self.landsat_dir = self.base_dir / "landsat"
        self.csv_dir = self.base_dir / "csv"
        self.processed_dir = self.landsat_dir / "processed"
        self.create_directories()
        self.target_crs = target_crs
        self.reference_crs = None
    
    def create_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            self.base_dir,
            self.landsat_dir,
            self.csv_dir,
            self.processed_dir
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            
    def get_landsat_path(self, location_id, scene_id, band):
        """Get path for a specific Landsat band file"""
        return self.processed_dir / f"SR_{location_id}_{scene_id}_{band}.tif"
    
    def get_csv_path(self, filename):
        """Get path for a CSV file"""
        return self.csv_dir / filename
    
    def clean_directories(self):
        """Remove temporary files and empty directories"""
        # Add cleanup logic here if needed
        pass
    
    def get_reference_crs(self, geotiff_path):
        """Get CRS from a reference GeoTIFF file"""
        with rasterio.open(geotiff_path) as src:
            self.reference_crs = src.crs
            return self.reference_crs
    
    def set_target_crs(self, crs):
        """Set target CRS for all outputs"""
        self.target_crs = crs
    
    def get_target_crs(self):
        """Get target CRS, falling back to reference CRS if not set"""
        return self.target_crs if self.target_crs else self.reference_crs
