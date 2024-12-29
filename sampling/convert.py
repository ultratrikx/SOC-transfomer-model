import pandas as pd
import pyproj
import folium

# Read the CSV file
# Assuming your CSV has columns named 'x' and 'y' for coordinates
df = pd.read_csv('./spatially_diverse_points_25000.csv')

# Set up the coordinate transformation
source = pyproj.CRS('EPSG:3347')
target = pyproj.CRS('EPSG:4326')
transformer = pyproj.Transformer.from_crs(source, target, always_xy=True)

# Convert coordinates - swapping the order since input is lat/lon
lat_lon = [transformer.transform(row['lon'], row['lat']) for _, row in df.iterrows()]
df['longitude'] = [coord[0] for coord in lat_lon]
df['latitude'] = [coord[1] for coord in lat_lon]

# Drop original lat/lon columns
df.drop(['lat', 'lon'], axis=1, inplace=True)

# Save converted coordinates
df.to_csv('converted_coordinates_25000.csv', index=False)
