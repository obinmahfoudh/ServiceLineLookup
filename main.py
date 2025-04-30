from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import geopandas as gpd
from scipy.spatial import cKDTree
import numpy as np

app = FastAPI()

# CORS (allow frontend to call backend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Later you can restrict this to your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load service line points and create CDK tree
print("Loading service lines...")
gdf = gpd.read_file("/var/data/service_line.geojson")
coords = np.array(list(zip(gdf.geometry.x, gdf.geometry.y)))  # lon, lat
important_columns = [
    "Address",
    "PWS-Owned Service Line Material",
    "Customer Side Service Line Material",
    "Source of Information Used for Service Line Identification - PWS Side",
    "Source of Information Used for Service Line Identification - Customer",
    "geometry"
]

gdf = gdf[important_columns]

tree = cKDTree(coords)
print("CDK Tree built.")

# Create a fast lookup: address -> list of row indices
address_to_indices = {}

for idx, address in enumerate(gdf["Address"]):
    if address not in address_to_indices:
        address_to_indices[address] = []
    address_to_indices[address].append(idx)

# Store the GeoDataFrame too for lookup
gdf.reset_index(inplace=True)

@app.get("/")
def root():
    return {"message": "Service Line API is running."}

@app.get("/nearest")
def nearest(lon: float, lat: float, k: int = 2):
    """Find nearest service lines, dynamically handling multiple service lines at same address."""
    distances, indices = tree.query([(lon, lat)], k=k)

    # Fix: Ensure first_idx is a single row index
    if isinstance(indices[0], (np.integer, int)):
        first_idx = indices[0]
    else:
        first_idx = indices[0][0]

    target_address = gdf.iloc[first_idx]["Address"]
    matching_indices = address_to_indices.get(target_address, [])
    results= []

    if len(matching_indices) > 1:
        # Multiple service lines at this address: return them all
        for idx in matching_indices:
            row = gdf.iloc[idx]
            results.append({
                "address": str(row["Address"]),
                "public_material": str(row["PWS-Owned Service Line Material"]),
                "private_material": str(row["Customer Side Service Line Material"]),
                "public_verification": str(row["Source of Information Used for Service Line Identification - PWS Side"]),
                "private_verification": str(row["Source of Information Used for Service Line Identification - Customer"]),
                "latitude": float(row.geometry.y),
                "longitude": float(row.geometry.x),
                "distance": 0.0
            })

    else:
        # Only a single service line: return top k nearest
        for dist, idx in zip(distances, indices):
            row = gdf.iloc[idx]
            results.append({
                "address": str(row["Address"]),
                "public_material": str(row["PWS-Owned Service Line Material"]),
                "private_material": str(row["Customer Side Service Line Material"]),
                "public_verification": str(row["Source of Information Used for Service Line Identification - PWS Side"]),
                "private_verification": str(row["Source of Information Used for Service Line Identification - Customer"]),
                "latitude": float(row.geometry.y),
                "longitude": float(row.geometry.x),
                "distance": float(dist)
            })


    return {"nearest_lines": results}