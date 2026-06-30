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

# --- Vessel Speed Threshold ---
MIN_TRACKING_SPEED_KNOTS = 4.0


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
    Manages a persistent, shared model of the river zones with dynamic Dead Reckoning.
    """
    def __init__(self):
        self.zones = {i: {} for i in range(1, 12)}
        self.lock = asyncio.Lock()

    async def remove_ship(self, mmsi, name, exit_reason):
        """
        Instantly clears a ship from all tracking zones when it leaves the river perimeter or docks.
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

    async def update_ship(self, mmsi, name, zone_num, direction, current_lat, sog):
        """
        Updates ship data when a live API message is received, calculating simulation offset.
        """
        async with self.lock:
            now = datetime.now(timezone.utc)
            
            # Look for existing tracking instance to calculate offset before we update state
            existing_ship_data = None
            for z_idx, ships in self.zones.items():
                if mmsi in ships:
                    existing_ship_data = ships[mmsi]
                    if z_idx != zone_num:
                        del ships[mmsi]
                        print(f"[{now}] {name} (MMSI: {mmsi}) moved out of Zone {z_idx}")
                    break

            # AIS stream fallback: 102.3 means speed unavailable.
            valid_sog = sog if (sog is not None and sog < 102.2) else 6.0

            if existing_ship_data:
                # Calculate how far off our simulation was from reality
                simulated_lat = existing_ship_data["latitude"]
                lat_offset = current_lat - simulated_lat
                
                # Convert latitude degrees back to approx nautical miles (1 degree latitude ≈ 60 Nautical Miles)
                nm_offset = lat_offset * 60.0
                
                print(f"[{now}] [OFFSET REPORT] {name} (MMSI: {mmsi}) "
                      f"Simulated Lat: {simulated_lat:.5f} vs Real Lat: {current_lat:.5f}. "
                      f"Offset: {lat_offset:+.5f}° ({nm_offset:+.4f} NM)")

            is_new = existing_ship_data is None
            
            # Add or update the ship data in the correct zone
            self.zones[zone_num][mmsi] = {
                "name": name,
                "direction": direction,
                "latitude": current_lat,
                "sog": valid_sog,
                "last_updated": now,
                "last_real_api_update": now,  # Anchors the exact time of the last true API data
                "is_dead_reckoning": True     # Set to True because we always simulate moving forward
            }
            
            action = "entered" if is_new else "updated in"
            print(f"[{now}] {name} {action} Zone {zone_num} heading {direction} at {valid_sog} kts")

    async def process_dead_reckoning_and_cleanup(self, max_dead_reckon_minutes=110):
        """
        Runs every 60 seconds. Always simulates movement for ALL current tracked ships
        using their last known SOG speed, and clears them completely if they hit an exit zone or timeout.
        """
        while True:
            await asyncio.sleep(60) 
            
            async with self.lock:
                now = datetime.now(timezone.utc)
                api_cutoff = now - timedelta(minutes=max_dead_reckon_minutes)
                
                ships_to_migrate = []  # format: (mmsi, old_zone, new_zone, data)
                ships_to_delete = []   # format: (mmsi, zone, reason)

                for zone_num, ships in self.zones.items():
                    for mmsi, data in list(ships.items()):
                        
                        # 1. Check absolute drop window (Hard Eviction)
                        if data["last_real_api_update"] < api_cutoff:
                            ships_to_delete.append((mmsi, zone_num, f"Absolute Timeout ({max_dead_reckon_minutes}m)"))
                            continue

                        # Math: Speed in knots / 3600 converts nautical miles/hr into degrees latitude/minute
                        ship_lat_change_per_minute = data["sog"] / 3600.0

                        # Always advance simulated latitude based on direction
                        if data["direction"] == "North":
                            data["latitude"] += ship_lat_change_per_minute
                        elif data["direction"] == "South":
                            data["latitude"] -= ship_lat_change_per_minute
                        else:
                            # Unknown heading: Cannot reliably calculate positioning path, hold position.
                            continue

                        # Re-verify zone layout using the freshly simulated latitude coordinate
                        new_zone = get_zone_number(data["latitude"])
                        data["last_updated"] = now

                        if new_zone in ["EXIT_NORTH", "EXIT_SOUTH", None]:
                            ships_to_delete.append((mmsi, zone_num, f"Simulated exit via {new_zone}"))
                        elif new_zone != zone_num:
                            ships_to_migrate.append((mmsi, zone_num, new_zone, data))

                # Process Zone Migrations
                for mmsi, old_zone, new_zone, data in ships_to_migrate:
                    if mmsi in self.zones[old_zone]:
                        del self.zones[old_zone][mmsi]
                    self.zones[new_zone][mmsi] = data
                    print(f"[{now}] [SIMULATION] {data['name']} drifted from Zone {old_zone} into Zone {new_zone}")

                # Process Evictions
                for mmsi, zone, reason in ships_to_delete:
                    if mmsi in self.zones[zone]:
                        ship_name = self.zones[zone][mmsi]["name"]
                        del self.zones[zone][mmsi]
                        print(f"[{now}] REMOVED ship: {ship_name} (MMSI: {mmsi}) from Zone {zone}. Reason: {reason}")


async def connect_ais_stream(tracker: RiverTracker):
    uri = "wss://stream.aisstream.io/v0/stream"
    
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    retry_delay = 5  # Start with a 5-second delay on reconnect
    
    while True:
        try:
            print(f"[{datetime.now(timezone.utc)}] Attempting to connect to AISStream...")
            
            async with websockets.connect(
                uri, 
                ssl=ssl_context, 
                ping_interval=20, 
                ping_timeout=20
            ) as websocket:
                
                print(f"[{datetime.now(timezone.utc)}] Connected! Resetting retry delay.")
                retry_delay = 5  # Reset delay upon successful connection
                
                st_clair_river_bbox = [[AIS_LAT_MIN, LON_MIN], [AIS_LAT_MAX, LON_MAX]]
                subscribe_message = {
                    "APIKey": "",
                    "BoundingBoxes": [st_clair_river_bbox], 
                    "FilterMessageTypes": ["PositionReport"] 
                }

                await websocket.send(json.dumps(subscribe_message))
                print(f"[{datetime.now(timezone.utc)}] Subscription sent.")

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
                        sog = position_report.get('Sog')  # Pulling Speed Over Ground (knots)
                        
                        if latitude and mmsi:
                            # If the ship has slowed down below our threshold, remove it from the system entirely.
                            if sog is not None and sog < MIN_TRACKING_SPEED_KNOTS:
                                await tracker.remove_ship(
                                    mmsi, 
                                    ship_name, 
                                    exit_reason="Dropped below minimum tracking speed (likely docked/anchored)"
                                )
                                continue
                                
                            zone = get_zone_number(latitude)
                            
                            if zone in ["EXIT_NORTH", "EXIT_SOUTH"]:
                                await tracker.remove_ship(mmsi, ship_name, exit_reason=zone)
                            elif zone:
                                direction = get_direction(cog)
                                await tracker.update_ship(mmsi, ship_name, zone, direction, latitude, sog)

        except (websockets.exceptions.ConnectionClosed, ssl.SSLError, OSError) as e:
            print(f"[{datetime.now(timezone.utc)}] Connection lost/dropped: {e}")
        except Exception as e:
            print(f"[{datetime.now(timezone.utc)}] Unexpected error occurred: {e}")
        
        # Reconnection Backoff logic
        print(f"[{datetime.now(timezone.utc)}] Retrying connection in {retry_delay} seconds...")
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 60)


async def main():
    tracker = RiverTracker()
    await asyncio.gather(
        connect_ais_stream(tracker),
        tracker.process_dead_reckoning_and_cleanup(max_dead_reckon_minutes=45)
    )

if __name__ == "__main__":
    asyncio.run(main())
