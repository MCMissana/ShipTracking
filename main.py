import asyncio
import websockets
import json
import ssl
from datetime import datetime, timezone, timedelta

# --- Core Zone Configuration ---
TRACKING_LAT_MIN, TRACKING_LAT_MAX = 42.57846, 42.71884
LON_MIN, LON_MAX = -82.562256, -82.42218

TOTAL_LAT_SPAN = TRACKING_LAT_MAX - TRACKING_LAT_MIN
ZONE_HEIGHT = TOTAL_LAT_SPAN / 11

# --- Expanded Exit Box Configuration ---
AIS_LAT_MIN = TRACKING_LAT_MIN - 0.02 
AIS_LAT_MAX = TRACKING_LAT_MAX + 0.02

# --- Dead Reckoning Constants ---
# 6 knots average = 0.1 degrees latitude per hour = ~0.001667 degrees per minute
LAT_CHANGE_PER_MINUTE = 0.001667 

def get_zone_number(latitude):
    """
    Determines the zone number (1-11) or returns an Exit Box designator.
    """
    if TRACKING_LAT_MAX < latitude <= AIS_LAT_MAX:
        return "EXIT_NORTH"
    if AIS_LAT_MIN <= latitude < TRACKING_LAT_MIN:
        return "EXIT_SOUTH"
    if TRACKING_LAT_MIN <= latitude <= TRACKING_LAT_MAX:
        distance_from_top = TRACKING_LAT_MAX - latitude
        zone_index = int(distance_from_top // ZONE_HEIGHT)
        zone_number = min(zone_index + 1, 11)
        return zone_number
    return None 

def get_direction(cog):
    if cog is None:
        return "Unknown"
    if 90 < cog <= 270:
        return "South"
    return "North"


class RiverTracker:
    """
    Manages a persistent model of the river zones with Dead Reckoning fallback.
    """
    def __init__(self):
        self.zones = {i: {} for i in range(1, 12)}
        self.lock = asyncio.Lock()

    async def remove_ship(self, mmsi, name, exit_reason):
        async with self.lock:
            now = datetime.now(timezone.utc)
            removed = False
            for z_idx, ships in self.zones.items():
                if mmsi in ships:
                    del ships[mmsi]
                    print(f"[{now}] {name} (MMSI: {mmsi}) REMOVED via {exit_reason}.")
                    removed = True
            return removed

    async def update_ship(self, mmsi, name, zone_num, direction, current_lat):
        """
        Updates ship data when a live API message is received.
        """
        async with self.lock:
            now = datetime.now(timezone.utc)
            
            # Enforce that a ship only ever exists in ONE zone
            for z_idx, ships in self.zones.items():
                if mmsi in ships and z_idx != zone_num:
                    del ships[mmsi]

            self.zones[zone_num][mmsi] = {
                "name": name,
                "direction": direction,
                "latitude": current_lat,
                "last_updated": now,
                "last_real_api_update": now,  # Permanent mark of last true data
                "is_dead_reckoning": False
            }
            print(f"[{now}] {name} (MMSI: {mmsi}) API update in Zone {zone_num} heading {direction}")

    async def process_dead_reckoning_and_cleanup(self, max_dead_reckon_minutes=45):
        """
        Runs every minute. Simulates movement for ships missing updates,
        and clears them out if they hit an exit zone or exceed global timeout.
        """
        while True:
            await asyncio.sleep(60) # Run simulation step once per minute
            
            async with self.lock:
                now = datetime.now(timezone.utc)
                api_cutoff = now - timedelta(minutes=max_dead_reckon_minutes)
                live_cutoff = now - timedelta(minutes=4) # 4 mins without API means start mimicking
                
                # We will gather moves and deletes to execute outside the dictionary iteration loop
                ships_to_migrate = [] # list of tuples: (mmsi, current_zone, new_zone, data)
                ships_to_delete = []  # list of tuples: (mmsi, zone, reason)

                for zone_num, ships in self.zones.items():
                    for mmsi, data in list(ships.items()):
                        # Step 1: Check absolute max expiration (Hard Eviction)
                        if data["last_real_api_update"] < api_cutoff:
                            ships_to_delete.append((mmsi, zone_num, "Absolute Timeout (45m)"))
                            continue

                        # Step 2: Determine if ship needs dead reckoning (No API updates for > 4 mins)
                        if data["last_real_api_update"] < live_cutoff:
                            if not data["is_dead_reckoning"]:
                                print(f"[{now}] ALERT: Lost API for {data['name']}. Starting dead reckoning...")
                                data["is_dead_reckoning"] = True

                            # Mimic movement based on direction
                            if data["direction"] == "North":
                                data["latitude"] += LAT_CHANGE_PER_MINUTE
                            elif data["direction"] == "South":
                                data["latitude"] -= LAT_CHANGE_PER_MINUTE
                            else:
                                # Direction is unknown, we can't safely mimic movement. Hold position till timeout.
                                continue

                            # Re-evaluate zone based on simulated latitude
                            new_zone = get_zone_number(data["latitude"])
                            data["last_updated"] = now

                            if new_zone in ["EXIT_NORTH", "EXIT_SOUTH", None]:
                                ships_to_delete.append((mmsi, zone_num, f"Dead Reckoned out via {new_zone}"))
                            elif new_zone != zone_num:
                                ships_to_migrate.append((mmsi, zone_num, new_zone, data))

                # Apply migrations (Zone transitions)
                for mmsi, old_zone, new_zone, data in ships_to_migrate:
                    if mmsi in self.zones[old_zone]:
                        del self.zones[old_zone][mmsi]
                    self.zones[new_zone][mmsi] = data
                    print(f"[{now}] [SIMULATION] {data['name']} moved from Zone {old_zone} to Zone {new_zone}")

                # Apply deletions
                for mmsi, zone, reason in ships_to_delete:
                    if mmsi in self.zones[zone]:
                        ship_name = self.zones[zone][mmsi]["name"]
                        del self.zones[zone][mmsi]
                        print(f"[{now}] REMOVED ship: {ship_name} from Zone {zone}. Reason: {reason}")


async def connect_ais_stream(tracker: RiverTracker):
    uri = "wss://stream.aisstream.io/v0/stream"
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    retry_delay = 5 
    
    while True:
        try:
            print(f"[{datetime.now(timezone.utc)}] Attempting to connect to AISStream...")
            async with websockets.connect(
                uri, ssl=ssl_context, ping_interval=20, ping_timeout=20
            ) as websocket:
                
                print(f"[{datetime.now(timezone.utc)}] Connected! Resetting retry delay.")
                retry_delay = 5 
                
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
                    if message.get("MessageType") == "PositionReport":
                        metadata = message.get("MetaData", {})
                        ship_name = metadata.get("ShipName", "UNKNOWN").strip()
                        mmsi = metadata.get("MMSI")
                        
                        position_report = message['Message']['PositionReport']
                        latitude = position_report.get('Latitude')
                        cog = position_report.get('Cog')
                        
                        if latitude and mmsi:
                            zone = get_zone_number(latitude)
                            if zone in ["EXIT_NORTH", "EXIT_SOUTH"]:
                                await tracker.remove_ship(mmsi, ship_name, exit_reason=zone)
                            elif zone:
                                direction = get_direction(cog)
                                # Pass latitude into update_ship to establish the baseline
                                await tracker.update_ship(mmsi, ship_name, zone, direction, latitude)

        except (websockets.exceptions.ConnectionClosed, ssl.SSLError, OSError) as e:
            print(f"[{datetime.now(timezone.utc)}] Connection lost/dropped: {e}")
        except Exception as e:
            print(f"[{datetime.now(timezone.utc)}] Unexpected error occurred: {e}")
        
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
