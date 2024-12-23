import os
import pandas as pd
from data_manager import DataManager
from v1.landsat_data_fetch import LandsatFetcher
from concurrent.futures import ThreadPoolExecutor, as_completed

def renew_session(landsat, username, token):
    """Renew the API session"""
    print("\nRenewing session...")
    landsat.logout()
    if not landsat.login(username, token):
        raise Exception("Failed to renew session")
    print("Session renewed successfully")

def main():
    # Initialize data manager with the master locations file directly
    master_csv = os.path.join("csv", "master_locations.csv")
    
    print(f"Loading data from: {master_csv}")
    locations_df = pd.read_csv(master_csv)
    
    # Initialize data manager with the master data
    data_manager = DataManager("csv\\data.csv")
    data_manager.data = locations_df  # Replace the data with master locations
    
    print(f"\nTotal records in dataset: {len(locations_df)}")
    print(f"Records with SoilGrids data: {len(locations_df[locations_df['soilgrids_id'].notna()])}")
    print("\nSample of data:")
    print(locations_df[['location_id', 'latitude', 'longitude', 'soilgrids_id']].head())
    
    # Replace with your ERS credentials
    username = "ArjunGupta"
    token = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"
    
    # Create Landsat fetcher instance with the loaded data
    landsat = LandsatFetcher(data_manager)
    
    if not landsat.login(username, token):
        return
    
    try:
        print("\nDownloading and processing Landsat data...")
        batch_size = 10  # Process 10 locations before renewing session
        locations_processed = 0
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for _, row in locations_df.iterrows():
                if pd.notna(row['soilgrids_id']) and str(row['soilgrids_id']).strip():
                    print(f"Processing location {row['location_id']} (soilgrids_id: {row['soilgrids_id']})...")
                    future = executor.submit(
                        landsat._process_single_coordinate,
                        row['latitude'],
                        row['longitude'],
                        row['location_id']
                    )
                    futures.append(future)
                    
                    # Renew session after each batch
                    locations_processed += 1
                    if locations_processed % batch_size == 0:
                        # Wait for current batch to complete
                        for f in futures[-batch_size:]:
                            try:
                                f.result()
                            except Exception as e:
                                print(f"Error processing Landsat data: {str(e)}")
                        renew_session(landsat, username, token)
            
            print(f"\nTotal locations to process: {len(futures)}")
            
            # Process any remaining futures
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing Landsat data: {str(e)}")
        
        # Get locations with complete data
        complete_data = data_manager.get_collocated_data()
        print(f"\nSuccessfully processed {len(complete_data)} locations with both datasets")
    
    finally:
        landsat.logout()

if __name__ == "__main__":
    main()
