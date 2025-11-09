from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import geopandas as gpd
from scipy.spatial import cKDTree
import numpy as np
import os

DEBUG = os.getenv("DEBUG", "false").lower() == "true"


app = FastAPI()

# CORS (allow frontend to call backend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load service line points and create CDK tree
print("Loading service lines...")
gdf = gpd.read_file("/var/data/service_line.geojson")
# lon, lat
coords = np.array(list(zip(gdf.geometry.x, gdf.geometry.y)))  
important_columns = [
    "Address",
    "Is this a high-risk Facility or Area?",
    "Gooseneck/ Pigtail",
    "PWS-Owned Service Line Material",
    "Customer Side Service Line Material",
    "Classification for Entire Service Line",
    "Source of Information Used for Service Line Identification - PWS Side",
    "Source of Information Used for Service Line Identification - Customer",
    "geometry"
]

gdf = gdf[important_columns]

tree = cKDTree(coords)
print("CDK Tree built.")

# Create a fast lookup: address -> list of row indices
address_to_indices = {}

print("Building fast lookup dictionary")
for idx, address in enumerate(gdf["Address"]):
    if address not in address_to_indices:
        address_to_indices[address] = []
    address_to_indices[address].append(idx)

# Store the GeoDataFrame too for lookup
gdf.reset_index(inplace=True)

# Haversine distance for better distance calculations, return feet for user clarity
def haversine_feet(lon1, lat1, lon2, lat2):
    R = 6371.0  # Earth radius in kilometers
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    distance_km = R * c
    distance_ft = distance_km * 3280.84  # Convert km to feet
    return distance_ft

@app.get("/")
def root():
    return {"message": "Service Line API is running."}

@app.get("/nearest")
def nearest(lon: float, lat: float, k: int = 2):
    """Find nearest service lines, dynamically handling multiple service lines at same address."""
    # Set matches higher so we can get k unique addresses
    matches = k * 10
    # Query tree and get indices and distances
    distances, indices = tree.query([(lon, lat)], k= matches)

    # Tree query returns list of list so flatten it
    distances = list(np.ravel(distances).astype(float))
    indices = list(np.ravel(indices).astype(int))
    # Lookup dictionary to get distance for certain index
    index_to_distance = dict(zip(indices, distances))

    # Get unique addresses and their distance 
    addresses = []
    #distance_fixed= []
    for index in indices:
        address = gdf.iloc[index]["Address"]
        if address not in addresses:
            addresses.append(address)
            #distance = index_to_distance.get(index, 0.0)
            #distance_fixed.append(distance)
        # Make sure its k unique addresses
        if len(addresses) >= k:
            break

    # Get all rows with our unique addresses
    matching_indices= []
    for address in addresses:
        matching_indices.append(address_to_indices.get(address))
    # Debug printing
    if DEBUG:
        print(f"Distance {distances} and indices {indices}")   
        print(f"Addresses: {addresses}")
        print(f"Matching indices: {matching_indices}")

    # Array containing our json output
    results= []

    # For each index return the row data we're interested in
    for array in matching_indices:
        for idx in array:
            row = gdf.iloc[idx]
            results.append({
                "address": str(row["Address"]),
                "gooseneck/pigtail": str(row["Gooseneck/ Pigtail"]),
                "public_material": str(row["PWS-Owned Service Line Material"]),
                "private_material": str(row["Customer Side Service Line Material"]),
                "classification for entire service line": str(row["Classification for Entire Service Line"]),
                "public_verification": str(row["Source of Information Used for Service Line Identification - PWS Side"]),
                "private_verification": str(row["Source of Information Used for Service Line Identification - Customer"]),
                "latitude": float(row.geometry.y),
                "longitude": float(row.geometry.x),
                "distance": float(haversine_feet(row.geometry.x, row.geometry.y, lon, lat))
            })

    return {"nearest_lines": results}
    