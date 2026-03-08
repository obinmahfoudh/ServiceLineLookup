from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncpg
import os

# Use the Transaction Pooler URL (Port 6543) from Supabase settings
DATABASE_URL = os.getenv("DATABASE_URL")

# Startup and Shutdown commands
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)    
    yield
    # Shutdown
    await app.state.pool.close()

app = FastAPI(lifespan = lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Service Line API is running"}

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
                SELECT "Address", 
                       -- We calculate the distance here so ORDER BY can see it
                       geometry <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography as dist
                FROM (
                    SELECT DISTINCT "Address", geometry 
                    FROM locations
                ) sub
                ORDER BY dist
                LIMIT $3
            )
            SELECT 
                l."Address",
                l."Gooseneck/ Pigtail",
                l."PWS-Owned Service Line Material",
                l."Customer Side Service Line Material",
                l."Classification for Entire Service Line",
                l."Source of Information Used for Service Line Identification - PWS Side",
                l."Source of Information Used for Service Line Identification - Customer",
                ST_X(l.geometry::geometry) as lon,
                ST_Y(l.geometry::geometry) as lat,
                ST_Distance(l.geometry, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography) * 3.28084 as dist_ft
            FROM locations l
            JOIN unique_addresses ua ON l."Address" = ua."Address"
            ORDER BY dist_ft ASC;
            """
        print(f"DEBUG: Searching for Lon: {lon}, Lat: {lat}, K: {k}")
        rows = await conn.fetch(query, lon, lat, k)
        print(f"DEBUG: Database returned {len(rows)} rows")

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