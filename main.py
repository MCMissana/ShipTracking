import asyncio
import websockets
import json
import ssl
from datetime import datetime, timezone, timedelta

# --- Zone Configuration ---
LAT_MIN, LAT_MAX = 42.57846, 42.71884
LON_MIN, LON_MAX = -82.562256, -82.42218

TOTAL_LAT_SPAN = LAT_MAX - LAT_MIN
ZONE_HEIGHT = TOTAL_LAT_SPAN / 11

def get_zone_number(latitude):
    """
    Determines the zone number (1-11) based on latitude.
    Zone 1 is the northernmost, Zone 11 is the southernmost.
    """
    if not (LAT_MIN <= latitude <= LAT_MAX):
        return None 
    
    distance_from_top = LAT_MAX - latitude
    zone_index = int(distance_from_top // ZONE_HEIGHT)
    zone_number = min(zone_index + 1, 11)
    return zone_number

def get_direction(cog):
    """
    Determines Northbound or Southbound based on Course Over Ground (COG).
    """
    if cog is None:
        return "Unknown"
    # COG is 0-360. 0/360 is true North. 
    # Broadly: 270 to 90 degrees is Northbound, 90 to 270 is Southbound
    if 90 < cog <= 270:
        return "South"
    return "North"


class RiverTracker:
    """
    Manages a persistent, shared model of the river zones.
    """
    def __init__(self):
        # Initialize zones 1 through 11 with empty tracking dicts
        self.zones = {i: {} for i in range(1, 12)}
        self.lock = asyncio.Lock()

    async def update_ship(self, mmsi, name, zone_num, direction):
        async with self.lock:
            now = datetime.now(timezone.utc)
            
            # 1. Enforce that a ship only ever exists in ONE zone.
            # Remove this MMSI if it currently resides in any other zone
            for z_idx, ships in self.zones.items():
                if mmsi in ships and z_idx != zone_num:
                    del ships[mmsi]
                    print(f"[{now}] {name} (MMSI: {mmsi}) moved out of Zone {z_idx}")

            # 2. Add or update the ship data in the correct zone
            is_new = mmsi not in self.zones[zone_num]
            self.zones[zone_num][mmsi] = {
                "name": name,
                "direction": direction,
                "last_updated": now
            }
            
            action = "entered" if is_new else "updated in"
            print(f"[{now}] {name} {action} Zone {zone_num} heading {direction}")

    async def cleanup_stale_ships(self, ttl_minutes=10):
        """
        Background worker that evicts ships missing updates for over `ttl_minutes`.
        """
        while True:
            await asyncio.sleep(10) # Check every 10 seconds
            async with self.lock:
                now = datetime.now(timezone.utc)
                cutoff = now - timedelta(minutes=ttl_minutes)
                
                for zone_num, ships in self.zones.items():
                    # Find all MMSIs matching eviction criteria
                    stale_mmsis = [
                        mmsi for mmsi, data in ships.items() 
                        if data["last_updated"] < cutoff
                    ]
                    
                    for mmsi in stale_mmsis:
                        ship_name = ships[mmsi]["name"]
                        del ships[mmsi]
                        print(f"[{now}] REMOVED stale ship: {ship_name} (MMSI: {mmsi}) from Zone {zone_num} due to inactivity.")

    def print_state(self):
        """Optional debugging helper to see what's currently in memory"""
        print("\n--- Current River State ---")
        for zone_num, ships in self.zones.items():
            if ships:
                print(f"Zone {zone_num}:")
                for mmsi, data in ships.items():
                    print(f"  - [{data['direction']}] {data['name']} (MMSI: {mmsi}) - Last updated: {data['last_updated'].strftime('%H:%M:%S')}")
        print("---------------------------\n")


async def connect_ais_stream(tracker: RiverTracker):
    uri = "wss://stream.aisstream.io/v0/stream"
    
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    print(f"[{datetime.now(timezone.utc)}] Connecting to AISStream...")
    
    try:
        async with websockets.connect(uri, ssl=ssl_context) as websocket:
            st_clair_river_bbox = [[LAT_MIN, LON_MIN], [LAT_MAX, LON_MAX]]

            subscribe_message = {
                "APIKey": "",
                "BoundingBoxes": [st_clair_river_bbox], 
                "FilterMessageTypes": ["PositionReport"] 
            }

            await websocket.send(json.dumps(subscribe_message))
            print(f"[{datetime.now(timezone.utc)}] Subscription sent for St. Clair River delta region.")

            async for message_json in websocket:
                message = json.loads(message_json)
                message_type = message.get("MessageType")

                if message_type == "PositionReport":
                    metadata = message.get("MetaData", {})
                    ship_name = metadata.get("ShipName", "UNKNOWN").strip()
                    mmsi = metadata.get("MMSI")
                    
                    position_report = message['Message']['PositionReport']
                    latitude = position_report.get('Latitude')
                    cog = position_report.get('Cog') # Course over ground
                    
                    if latitude and mmsi:
                        zone = get_zone_number(latitude)
                        if zone:
                            direction = get_direction(cog)
                            # Update our persistent shared object
                            await tracker.update_ship(mmsi, ship_name, zone, direction)
                        else:
                            pass # Ignored outside bounding box boundaries
                            
                elif message_type == "Ping":
                    # Keepalive message handling if necessary
                    pass

    except Exception as e:
        print(f"Connection error occurred: {e}")

async def main():
    # Instantiate the shared tracker object
    tracker = RiverTracker()
    
    # Run the stream listener and the eviction loop concurrently
    await asyncio.gather(
        connect_ais_stream(tracker),
        tracker.cleanup_stale_ships(ttl_minutes=5)
    )

if __name__ == "__main__":
    asyncio.run(main())
