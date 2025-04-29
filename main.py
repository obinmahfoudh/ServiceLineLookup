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
gdf = gpd.read_file("service_lines.geojson")
coords = np.array(list(zip(gdf.geometry.x, gdf.geometry.y)))  # lon, lat
tree = cKDTree(coords)
print("CDK Tree built.")

# Store the GeoDataFrame too for lookup
gdf.reset_index(inplace=True)

@app.get("/")
def root():
    return {"message": "Service Line API is running."}

@app.get("/nearest")
def nearest(lon: float, lat: float, k: int = 2):
    """Find k nearest service lines to a given point."""
    distances, indices = tree.query([(lon, lat)], k=k)
    results = []
    for dist, idx in zip(distances, indices):
        row = gdf.iloc[idx]
        results.append({
            "address": row["address"],
            "material": row["material"],
            "latitude": row.geometry.y,
            "longitude": row.geometry.x,
            "distance": dist
        })
    return {"nearest_lines": results}
