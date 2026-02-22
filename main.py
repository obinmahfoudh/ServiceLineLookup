from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncpg
import os

# Use the Transaction Pooler URL (Port 6543) from Supabase settings
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# We use a connection pool so we don't open/close connections constantly
@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)

@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()

@app.get("/")
async def root():
    return {"message": "Service Line API is running via PostGIS."}

@app.get("/nearest")
async def nearest(lon: float, lat: float, k: int = 2):
    """
    Finds k unique addresses nearest to the point, then returns all service 
    lines associated with those addresses.
    """
    async with app.state.pool.acquire() as conn:
        # SQL LOGIC:
        # 1. The Subquery finds the k unique addresses closest to the user.
        # 2. The Main Query joins those addresses back to the table to get ALL lines at those addresses.
        # 3. ST_Distance calculates the distance in METERS
        query = """
            WITH unique_addresses AS (
                SELECT DISTINCT "Address"
                FROM locations
                ORDER BY geometry <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                LIMIT $3
            )
            SELECT 
                "Address",
                "Gooseneck/ Pigtail",
                "PWS-Owned Service Line Material",
                "Customer Side Service Line Material",
                "Classification for Entire Service Line",
                "Source of Information Used for Service Line Identification - PWS Side",
                "Source of Information Used for Service Line Identification - Customer",
                ST_Y(geometry::geometry) as lat,
                ST_X(geometry::geometry) as lon,
                ST_Distance(geometry, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography) as dist
            FROM locations
            WHERE "Address" IN (SELECT "Address" FROM unique_addresses)
            ORDER BY dist_ft ASC;
        """
        
        rows = await conn.fetch(query, lon, lat, k)
        
        results = []
        for row in rows:
            results.append({
                "address": str(row["Address"]),
                "gooseneck/pigtail": str(row["Gooseneck/ Pigtail"]),
                "public_material": str(row["PWS-Owned Service Line Material"]),
                "private_material": str(row["Customer Side Service Line Material"]),
                "classification for entire service line": str(row["Classification for Entire Service Line"]),
                "public_verification": str(row["Source of Information Used for Service Line Identification - PWS Side"]),
                "private_verification": str(row["Source of Information Used for Service Line Identification - Customer"]),
                "latitude": float(row["lat"]),
                "longitude": float(row["lon"]),
                "distance": float(row["dist"])
            })

        return {"nearest_lines": results}