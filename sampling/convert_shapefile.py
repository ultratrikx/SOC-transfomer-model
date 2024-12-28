import geopandas as gpd
import logging
import numpy as np
from fiona.crs import from_epsg
import fiona
from pathlib import Path
import pyproj

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_lambert_crs():
    """Create custom Lambert Conformal Conic CRS"""
    proj_params = {
        'proj': 'lcc',  # Lambert Conformal Conic
        'datum': 'NAD27',
        'ellps': 'clrk66',  # Clarke 1866
        'units': 'm',
        'lat_1': 49,  # 1st standard parallel
        'lat_2': 77,  # 2nd standard parallel
        'lat_0': 0,   # Latitude of origin
        'lon_0': -91.866667,  # Central meridian (-91° 52')
        'x_0': 0,     # False easting
        'y_0': 0      # False northing
    }
    return pyproj.CRS.from_dict(proj_params)

def detect_crs(shapefile_path):
    """Attempt to detect CRS from shapefile"""
    try:
        # First try the custom Lambert projection
        logger.info("Using custom Lambert Conformal Conic projection parameters...")
        return create_lambert_crs()
        
    except Exception as e:
        logger.warning(f"Error creating custom CRS: {e}")
        # Fall back to existing detection methods
        try:
            # Try reading .prj file directly
            prj_path = Path(shapefile_path).with_suffix('.prj')
            if (prj_path.exists()):
                with open(prj_path, 'r') as prj_file:
                    prj_text = prj_file.read()
                    try:
                        crs = pyproj.CRS.from_wkt(prj_text)
                        return crs.to_epsg()
                    except Exception as e:
                        logger.warning(f"Could not parse .prj file: {e}")
            
            # Try using fiona as backup
            with fiona.open(shapefile_path) as src:
                if src.crs:
                    return src.crs.get('init', '').upper()
        
        except Exception as e:
            logger.warning(f"Error detecting CRS: {e}")
    return None

def convert_shapefile(input_file, output_file, default_crs="EPSG:3857"):
    """Convert shapefile to EPSG:4326 (WGS84 lat/long format)"""
    try:
        logger.info(f"Loading shapefile: {input_file}")
        gdf = gpd.read_file(input_file)
        
        # Handle duplicate columns by removing them
        logger.info("Checking for duplicate columns...")
        if len(gdf.columns[gdf.columns.duplicated()]) > 0:
            logger.info("Found duplicate columns, keeping first occurrence only...")
            gdf = gdf.loc[:, ~gdf.columns.duplicated()]
        
        # Truncate column names to 10 characters for ESRI Shapefile compatibility
        logger.info("Truncating long column names...")
        rename_dict = {}
        for col in gdf.columns:
            if len(col) > 10 and col != 'geometry':
                new_name = col[:10]
                # Ensure no duplicates after truncation
                counter = 1
                while new_name in rename_dict.values():
                    new_name = f"{col[:7]}_{counter}"
                    counter += 1
                rename_dict[col] = new_name
        
        if rename_dict:
            logger.info(f"Renaming columns: {rename_dict}")
            gdf = gdf.rename(columns=rename_dict)
        
        # Convert numeric columns to appropriate types and handle large numbers
        for col in gdf.select_dtypes(include=['float64', 'int64']).columns:
            # Check if values are large
            if gdf[col].abs().max() > 1e8:  # If values are larger than 100 million
                logger.info(f"Converting large numbers in column {col} to scientific notation")
                gdf[col] = gdf[col].astype('str')  # Convert to string to preserve precision
        
        # Handle CRS conversion
        logger.info(f"Original CRS: {gdf.crs}")
        if gdf.crs is None:
            custom_crs = detect_crs(input_file)
            if custom_crs:
                logger.info(f"Using custom Lambert Conformal Conic CRS")
                gdf.set_crs(custom_crs, inplace=True)
            else:
                raise ValueError("Could not determine CRS")
        
        logger.info("Converting to EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)
        
        # Save converted file
        gdf.to_file(output_file)
        logger.info(f"Saved converted shapefile to: {output_file}")
        
        bounds = gdf.total_bounds
        logger.info(f"Bounds (lat/long): xmin={bounds[0]}, ymin={bounds[1]}, xmax={bounds[2]}, ymax={bounds[3]}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error converting shapefile: {str(e)}")
        return False

def main():
    input_file = './peatlands/peat032005.shp'
    output_file = './peatlands/peat032005_wgs84.shp'
    
    if convert_shapefile(input_file, output_file):
        logger.info("Conversion completed successfully")
    else:
        logger.error("Conversion failed")

if __name__ == "__main__":
    main()
