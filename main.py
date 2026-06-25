import asyncio
import websockets
import json
import ssl
from datetime import datetime, timezone, timedelta

# --- Core Zone Configuration ---
# The active tracking bounds
TRACKING_LAT_MIN, TRACKING_LAT_MAX = 42.57846, 42.71884
LON_MIN, LON_MAX = -82.562256, -82.42218

TOTAL_LAT_SPAN = TRACKING_LAT_MAX - TRACKING_LAT_MIN
ZONE_HEIGHT = TOTAL_LAT_SPAN / 11

# --- Expanded Exit Box Configuration ---
# Broaden the latitude search box by ~0.02 degrees North and South to capture exits
AIS_LAT_MIN = TRACKING_LAT_MIN - 0.02 
AIS_LAT_MAX = TRACKING_LAT_MAX + 0.02

def get_zone_number(latitude):
    """
    Determines the zone number (1-11) or returns an Exit Box designator.
    Zone 1 is northernmost, Zone 11 is southernmost.
    """
    # 1. Check Northern Exit Box
    if TRACKING_LAT_MAX < latitude <= AIS_LAT_MAX:
        return "EXIT_NORTH"
        
    # 2. Check Southern Exit Box
    if AIS_LAT_MIN <= latitude < TRACKING_LAT_MIN:
        return "EXIT_SOUTH"
        
    # 3. Check Standard Tracking Zones
    if TRACKING_LAT_MIN <= latitude <= TRACKING_LAT_MAX:
        distance_from_top = TRACKING_LAT_MAX - latitude
        zone_index = int(distance_from_top // ZONE_HEIGHT)
        zone_number = min(zone_index + 1, 11)
        return zone_number
        
    return None 

def get_direction(cog):
    """
    Determines Northbound or Southbound based on Course Over Ground (COG).
    """
    if cog is None:
        return "Unknown"
    if 90 < cog <= 270:
        return "South"
    return "North"


class RiverTracker:
    """
    Manages a persistent, shared model of the river zones.
    """
    def __init__(self):
        self.zones = {i: {} for i in range(1, 12)}
        self.lock = asyncio.Lock()

    async def remove_ship(self, mmsi, name, exit_reason):
        """
        Instantly clears a ship from all tracking zones when it leaves the river perimeter.
        """
        async with self.lock:
            now = datetime.now(timezone.utc)
            removed = False
            for z_idx, ships in self.zones.items():
                if mmsi in ships:
                    del ships[mmsi]
                    print(f"[{now}] {name} (MMSI: {mmsi}) REMOVED via {exit_reason}.")
                    removed = True
            return removed

    async def update_ship(self, mmsi, name, zone_num, direction):
        async with self.lock:
            now = datetime.now(timezone.utc)
            
            # Enforce that a ship only ever exists in ONE zone.
            for z_idx, ships in self.zones.items():
                if mmsi in ships and z_idx != zone_num:
                    del ships[mmsi]
                    print(f"[{now}] {name} (MMSI: {mmsi}) moved out of Zone {z_idx}")

            # Add or update the ship data in the correct zone
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
            await asyncio.sleep(10) 
            async with self.lock:
                now = datetime.now(timezone.utc)
                cutoff = now - timedelta(minutes=ttl_minutes)
                
                for zone_num, ships in self.zones.items():
                    stale_mmsis = [
                        mmsi for mmsi, data in ships.items() 
                        if data["last_updated"] < cutoff
                    ]
                    
                    for mmsi in stale_mmsis:
                        ship_name = ships[mmsi]["name"]
                        del ships[mmsi]
                        print(f"[{now}] REMOVED stale ship: {ship_name} (MMSI: {mmsi}) from Zone {zone_num} due to inactivity.")


async def connect_ais_stream(tracker: RiverTracker):
    uri = "wss://stream.aisstream.io/v0/stream"
    
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    print(f"[{datetime.now(timezone.utc)}] Connecting to AISStream...")
    
    try:
        async with websockets.connect(uri, ssl=ssl_context) as websocket:
            # Note: Using the expanded AIS limits here to catch ships entering exit boxes
            st_clair_river_bbox = [[AIS_LAT_MIN, LON_MIN], [AIS_LAT_MAX, LON_MAX]]

            subscribe_message = {
                "APIKey": "",
                "BoundingBoxes": [st_clair_river_bbox], 
                "FilterMessageTypes": ["PositionReport"] 
            }

            await websocket.send(json.dumps(subscribe_message))
            print(f"[{datetime.now(timezone.utc)}] Subscription sent for St. Clair River.")

            async for message_json in websocket:
                message = json.loads(message_json)
                message_type = message.get("MessageType")

                if message_type == "PositionReport":
                    metadata = message.get("MetaData", {})
                    ship_name = metadata.get("ShipName", "UNKNOWN").strip()
                    mmsi = metadata.get("MMSI")
                    
                    position_report = message['Message']['PositionReport']
                    latitude = position_report.get('Latitude')
                    cog = position_report.get('Cog')
                    
                    if latitude and mmsi:
                        zone = get_zone_number(latitude)
                        
                        if zone in ["EXIT_NORTH", "EXIT_SOUTH"]:
                            # Instantly drop the ship from tracking without waiting for TTL timeout
                            await tracker.remove_ship(mmsi, ship_name, exit_reason=zone)
                        elif zone:
                            direction = get_direction(cog)
                            await tracker.update_ship(mmsi, ship_name, zone, direction)
                            
                elif message_type == "Ping":
                    pass

    except Exception as e:
        print(f"Connection error occurred: {e}")

async def main():
    tracker = RiverTracker()
    await asyncio.gather(
        connect_ais_stream(tracker),
        tracker.cleanup_stale_ships(ttl_minutes=10)
    )

if __name__ == "__main__":
    async_instance = asyncio.run(main())
