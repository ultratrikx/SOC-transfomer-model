import os
from data_manager import DataManager
from v1.landsat_data_fetch import LandsatFetcher
from v1.soilgrids_data_fetch import SoilGridsFetcher
from concurrent.futures import ThreadPoolExecutor, as_completed

def main():
    # Initialize data manager
    data_manager = DataManager("csv\\data.csv")
    
    # Replace with your ERS credentials
    username = "ArjunGupta"
    token = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"
    
    # Create instances of fetchers
    landsat = LandsatFetcher(data_manager)
    soilgrids = SoilGridsFetcher(data_manager)
    
    if not landsat.login(username, token):
        return
    
    try:
        # First, process all SoilGrids data
        print("\nDownloading SoilGrids data...")
        for _, row in data_manager.data.iterrows():
            try:
                soilgrids.get_location_data(
                    row['latitude'],
                    row['longitude'],
                    row['location_id']
                )
                # Removed storage logging here
            except Exception as e:
                print(f"Error downloading SoilGrids data for location {row['location_id']}: {str(e)}")
        
        # Then process Landsat data
        print("\nDownloading and processing Landsat data...")
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for _, row in data_manager.data.iterrows():
                if row['soilgrids_id']:  # Only process if we have SoilGrids data
                    future = executor.submit(
                        landsat._process_single_coordinate,
                        row['latitude'],
                        row['longitude'],
                        row['location_id']
                    )
                    futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result()
                    # Removed storage logging here
                except Exception as e:
                    print(f"Error processing Landsat data: {str(e)}")
        
        # Removed final storage logging
        # Get locations with complete data
        complete_data = data_manager.get_collocated_data()
        print(f"\nSuccessfully processed {len(complete_data)} locations with both datasets")
    
    finally:
        landsat.logout()

if __name__ == "__main__":
    main()
