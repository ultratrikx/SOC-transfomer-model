import os
from pathlib import Path

class DataManager:
    def __init__(self, base_dir="data"):
        self.base_dir = Path(base_dir)
        self.landsat_dir = self.base_dir / "landsat"
        self.csv_dir = self.base_dir / "csv"
        self.processed_dir = self.landsat_dir / "processed"
        self.create_directories()
        
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
