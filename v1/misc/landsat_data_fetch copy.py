import pandas as pd
import requests
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
M2M_API_URL = "https://m2m.cr.usgs.gov/api/api/json/stable"  # Base URL for USGS M2M API
LOGIN_ENDPOINT = f"{M2M_API_URL}/login"
SEARCH_ENDPOINT = f"{M2M_API_URL}/scene-search"
DOWNLOAD_REQUEST_ENDPOINT = f"{M2M_API_URL}/download-request"  # Changed endpoint
DOWNLOAD_OPTIONS_ENDPOINT = f"{M2M_API_URL}/download-options"

# USERNAME = "rohanth"  # Replace with your Earthdata username
# PASSWORD = "fmv@azm-cvh4HKW-egm"  # Replace with your Earthdata password
USERNAME = "ArjunGupta"  # Replace with your Earthdata username
PASSWORD = "GJ86*gJKdv&i\"LM"  # Replace with your Earthdata password
# API_TOKEN = "wm0DyYKik@qdHwdYloSFZwsaJPITZ0aCPljss67jttT9@dVSsSxuDDxb_pJD9uga"
API_TOKEN = "RWHsOmS@KxK6lvrAVcj4N58L2Ub936ebsv@rpeibGyoA3ayArVU!gKLNqVnWE4br"

# Authenticate and get an API key
def get_api_key():
    payload = {
        "username": USERNAME,
        "password": PASSWORD
    }
    response = requests.post(LOGIN_ENDPOINT, json=payload)
    if response.status_code == 200:
        return response.json().get("data")
    else:
        print(f"Error during authentication: {response.status_code}, {response.text}")
        return None

def list_datasets(api_key):
    dataset_search_endpoint = f"{M2M_API_URL}/dataset-search"
    payload = {"datasetName": ""}
    headers = {"X-Auth-Token": api_key}
    response = requests.post(dataset_search_endpoint, json=payload, headers=headers)
    if response.status_code == 200:
        datasets = response.json().get("data", [])
        for dataset in datasets:
            collection_name = dataset.get("collectionName", "Unknown Collection")
            dataset_name = dataset.get("datasetAlias", "Unknown Dataset")
            print(f"{dataset_name} - {collection_name}")
            # Match the correct collection name
            if collection_name == "Landsat 8-9 OLI/TIRS C2 L2":
                print(f"Found matching dataset: {dataset_name} - {collection_name}")
                return dataset_name
        print("Desired collection not found in available datasets.")
    else:
        print(f"Error listing datasets: {response.status_code}, {response.text}")
    return None

def convert_to_wrs(latitude, longitude):
    """Convert lat/lon to approximate WRS-2 path/row"""
    # Fixed calculation for WRS-2
    path = int(((longitude + 180) % 360) / 360 * 233 + 1)
    row = 1 + ((60.0 - latitude) * 248.0 / 120.0)
    row = int(row + 0.5)  # Round to nearest integer
    return path, row

def get_neighboring_paths_rows(path, row):
    """Get a list of neighboring path/row combinations"""
    paths = [path-1, path, path+1]
    rows = [row-1, row, row+1]
    return [(p, r) for p in paths for r in rows]

def parse_metadata(metadata_list):
    """Convert metadata list to dictionary for easier access"""
    metadata_dict = {}
    for item in metadata_list:
        field_name = item.get('fieldName', '').replace(' ', '_').lower()
        value = item.get('value', '')
        metadata_dict[field_name] = value
    return metadata_dict

def point_in_polygon(point, polygon):
    """
    Ray casting algorithm to determine if a point is inside a polygon
    point: (lat, lon) tuple
    polygon: list of (lat, lon) tuples forming the polygon vertices
    """
    x, y = point
    n = len(polygon)
    inside = False
    
    p1x, p1y = polygon[0]
    for i in range(n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside

def is_point_in_scene(metadata, target_lat, target_lon):
    """Check if the point is within the scene's boundaries"""
    try:
        # Get corner coordinates
        corners = [
            (float(metadata.get('corner_upper_left_latitude', 0)), 
             float(metadata.get('corner_upper_left_longitude', 0))),
            (float(metadata.get('corner_upper_right_latitude', 0)), 
             float(metadata.get('corner_upper_right_longitude', 0))),
            (float(metadata.get('corner_lower_right_latitude', 0)), 
             float(metadata.get('corner_lower_right_longitude', 0))),
            (float(metadata.get('corner_lower_left_latitude', 0)), 
             float(metadata.get('corner_lower_left_longitude', 0)))
        ]
        
        # Add tolerance by expanding the polygon slightly
        center_lat = sum(lat for lat, _ in corners) / 4
        center_lon = sum(lon for _, lon in corners) / 4
        
        expanded_corners = []
        tolerance = 0.05  # approximately 5km
        for lat, lon in corners:
            # Expand corners away from center
            dlat = lat - center_lat
            dlon = lon - center_lon
            expanded_lat = lat + (dlat * tolerance)
            expanded_lon = lon + (dlon * tolerance)
            expanded_corners.append((expanded_lat, expanded_lon))
        
        # Debug print
        print(f"Scene boundaries (corners):")
        for i, (lat, lon) in enumerate(expanded_corners):
            print(f"  Corner {i+1}: {lat:.4f}, {lon:.4f}")
        print(f"  Target point: {target_lat:.4f}, {target_lon:.4f}")
        
        # Check if point is within expanded polygon
        is_within = point_in_polygon((target_lat, target_lon), expanded_corners)
        print(f"  Point is {'within' if is_within else 'outside'} scene boundaries")
        return is_within
        
    except (ValueError, TypeError) as e:
        print(f"Error checking scene boundaries: {e}")
        print("Metadata values:", {k: v for k, v in metadata.items() if 'corner' in k.lower()})
        return False

# Search for Landsat scenes
def search_landsat_granules(api_key, latitude, longitude, dataset="landsat_ot_c2_l2"):
    path, row = convert_to_wrs(longitude, latitude)
    print(f"\nCalculated WRS-2 Path/Row: {path}/{row}")
    
    # Get neighboring paths and rows
    path_rows = get_neighboring_paths_rows(path, row)
    
    payload = {
        "datasetName": dataset,
        "spatialFilter": {
            "filterType": "mbr",
            "lowerLeft": {
                "latitude": latitude - 1,
                "longitude": longitude - 1
            },
            "upperRight": {
                "latitude": latitude + 1,
                "longitude": longitude + 1
            }
        },
        "metadataType": "full",
        "maxResults": 100,
        "sortOrder": "DESC",
        "temporalFilter": {
            "start": "2023-01-01",
            "end": "2024-10-31"
        },
        "additionalCriteria": {
            "filterType": "or",
            "childFilters": [
                {
                    "filterType": "and",
                    "childFilters": [
                        {
                            "filterType": "value",
                            "field": "wrs_path",
                            "value": str(p).zfill(3),
                            "operand": "="
                        },
                        {
                            "filterType": "value",
                            "field": "wrs_row",
                            "value": str(r).zfill(3),
                            "operand": "="
                        }
                    ]
                } for p, r in path_rows
            ]
        }
    }
    
    print(f"Searching for scenes near lat: {latitude}, lon: {longitude}")
    print(f"Checking WRS-2 paths/rows: {path_rows}")
    
    headers = {
        "X-Auth-Token": api_key,
        "Content-Type": "application/json"
    }
    
    print(f"\nSearching for scenes near lat: {latitude}, lon: {longitude}")
    response = requests.post(SEARCH_ENDPOINT, json=payload, headers=headers)
    
    if response.status_code == 200:
        try:
            scenes = response.json().get("data", {}).get("results", [])
            valid_scenes = []
            
            for scene in scenes:
                metadata = parse_metadata(scene.get('metadata', []))
                
                # Only consider scenes that contain the target point
                if is_point_in_scene(metadata, latitude, longitude):
                    try:
                        cloud_cover = float(scene.get('cloudCover', 100))
                        acquisition_date = metadata.get('date_acquired', '')
                        
                        valid_scenes.append({
                            'scene': scene,
                            'cloud_cover': cloud_cover,
                            'date': acquisition_date
                        })
                        
                        print(f"Found valid scene:")
                        print(f"  Cloud cover: {cloud_cover}%")
                        print(f"  Date: {acquisition_date}")
                        print(f"  Entity ID: {scene['entityId']}")
                        print(f"  Display ID: {scene['displayId']}")
                        
                    except (ValueError, TypeError) as e:
                        print(f"Error processing scene: {e}")
                        continue
            
            if valid_scenes:
                # Sort by cloud cover and select the best scene
                best_scene = min(valid_scenes, key=lambda x: x['cloud_cover'])
                print(f"\nSelected best scene:")
                print(f"  Entity ID: {best_scene['scene']['entityId']}")
                print(f"  Cloud cover: {best_scene['cloud_cover']}%")
                print(f"  Date: {best_scene['date']}")
                return best_scene['scene']
            
            print(f"No valid scenes found containing point lat: {latitude}, lon: {longitude}")
            return None
            
        except Exception as e:
            print(f"Error parsing response: {e}")
            return None
    else:
        print(f"Error during search: {response.status_code}, {response.text}")
        return None

# Retrieve product ID
def get_product_id(api_key, entity_id, dataset):
    payload = {
        "datasetName": dataset,
        "entityIds": [entity_id],
        "downloadApplication": "EE"  # Specify Earth Explorer as download application
    }
    headers = {
        "X-Auth-Token": api_key,
        "Content-Type": "application/json"
    }
    response = requests.post(DOWNLOAD_OPTIONS_ENDPOINT, json=payload, headers=headers)
    if response.status_code == 200:
        products = response.json().get("data", [])
        if products:
            # Select the first available product ID
            return products[0]["id"]
        else:
            print("No products available for this scene.")
            return None
    else:
        print(f"Error retrieving product ID: {response.status_code}, {response.text}")
        return None

# Modified download function
def download_landsat_image(api_key, entity_id, product_id, output_folder, filename):
    payload = {
        "downloads": [{
            "entityId": entity_id,
            "productId": product_id
        }]
    }
    headers = {"X-Auth-Token": api_key}
    
    response = requests.post(DOWNLOAD_REQUEST_ENDPOINT, json=payload, headers=headers)
    if response.status_code == 200:
        available_downloads = response.json().get("data", {}).get("availableDownloads", [])
        if available_downloads:
            download_url = available_downloads[0].get("url")
            if download_url:
                # Create a simpler filename using just the entity_id
                file_name = os.path.join(output_folder, filename)
                print(f"Downloading: {download_url}")
                
                # Stream the download with progress bar
                file_response = requests.get(download_url, stream=True)
                if file_response.status_code == 200:
                    total_size = int(file_response.headers.get('content-length', 0))
                    
                    with open(file_name, "wb") as f:
                        pbar = tqdm(
                            total=total_size,
                            desc=f"Downloading {entity_id}",
                            unit='B',
                            unit_scale=True,
                            unit_divisor=1024,
                            dynamic_ncols=True
                        )
                        for data in file_response.iter_content(chunk_size=1024):
                            size = f.write(data)
                            pbar.update(size)
                        pbar.close()
                    print(f"Saved to {file_name}")
                else:
                    print(f"Failed to download file: {file_response.status_code}")
            else:
                print("No download URL found.")
        else:
            print("No available downloads found in response.")
    else:
        print(f"Error during download request: {response.status_code}, {response.text}")

def process_coordinate(api_key, latitude, longitude, dataset_name, output_folder):
    print(f"Processing lat: {latitude}, lon: {longitude}")
    scene = search_landsat_granules(api_key, latitude, longitude, dataset_name)
    if scene:
        entity_id = scene["entityId"]
        product_id = get_product_id(api_key, entity_id, dataset_name)
        if product_id:
            filename = f"landsat_{entity_id}_lat{latitude:.4f}_lon{longitude:.4f}.tar.gz"
            print(f"Downloading scene for Entity ID: {entity_id}")
            download_landsat_image(api_key, entity_id, product_id, output_folder, filename)
        else:
            print(f"No valid product found for Entity ID: {entity_id}")
    else:
        print(f"No granules found for lat: {latitude}, lon: {longitude}")

def fetch_landsat_images(csv_file, output_folder):
    api_key = get_api_key()
    if not api_key:
        print("Failed to authenticate.")
        return

    dataset_name = "landsat_ot_c2_l2"
    print(f"Using dataset: {dataset_name}")

    data = pd.read_csv(csv_file)

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Executor for downloads
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        # Perform searches sequentially
        for index, row in data.iterrows():
            latitude = row["latitude"]
            longitude = row["longitude"]

            print(f"Processing lat: {latitude}, lon: {longitude}")
            scene = search_landsat_granules(api_key, latitude, longitude, dataset_name)
            if scene:
                entity_id = scene["entityId"]
                display_id = scene["displayId"]
                print(f"Found scene with Entity ID: {entity_id}, Display ID: {display_id}")
                product_id = get_product_id(api_key, entity_id, dataset_name)
                if product_id:
                    filename = f"landsat_{entity_id}_lat{latitude:.4f}_lon{longitude:.4f}.tar.gz"
                    # Submit download task to executor
                    future = executor.submit(
                        download_landsat_image,
                        api_key,
                        entity_id,
                        product_id,
                        output_folder,
                        filename
                    )
                    futures.append(future)
                    print(f"Scheduled download for Entity ID: {entity_id}")
                else:
                    print(f"No valid product found for Entity ID: {entity_id}")
            else:
                print(f"No scenes found for lat: {latitude}, lon: {longitude}")

        # Wait for all downloads to complete
        for future in as_completed(futures):
            future.result()

# Example usage
if __name__ == "__main__":
    csv_file = "north_american_forests.csv"  # Path to your CSV file with 'latitude' and 'longitude' columns
    output_folder = "landsat_images"  # Folder to save downloaded images
    fetch_landsat_images(csv_file, output_folder)
