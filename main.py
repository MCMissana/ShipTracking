import asyncio
import websockets
import json
import ssl
from datetime import datetime, timezone

# --- Zone Configuration ---
# Main Bounding Box limits
LAT_MIN, LAT_MAX = 42.57846, 42.71884
LON_MIN, LON_MAX = -82.562256, -82.42218

# Calculate height of each zone
TOTAL_LAT_SPAN = LAT_MAX - LAT_MIN
ZONE_HEIGHT = TOTAL_LAT_SPAN / 11

def get_zone_number(latitude):
    """
    Determines the zone number (1-11) based on latitude.
    Zone 1 is the highest (northernmost), Zone 11 is the lowest (southernmost).
    """
    if not (LAT_MIN <= latitude <= LAT_MAX):
        return None # Outside the bounding box boundaries
    
    # Calculate how far down from the maximum latitude the ship is
    distance_from_top = LAT_MAX - latitude
    
    # Floor division determines the zone index (0 to 10)
    zone_index = int(distance_from_top // ZONE_HEIGHT)
    
    # Edge case: If latitude is exactly LAT_MIN, it might evaluate to index 11
    zone_number = min(zone_index + 1, 11)
    return zone_number

async def connect_ais_stream():
    uri = "wss://stream.aisstream.io/v0/stream"
    
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    print(f"[{datetime.now(timezone.utc)}] Connecting to AISStream...")
    
    try:
        async with websockets.connect(uri, ssl=ssl_context) as websocket:
            st_clair_river_bbox = [[LAT_MIN, LON_MIN], [LAT_MAX, LON_MAX]]

            # Subscribe to PositionReport for live tracking
            subscribe_message = {
                "APIKey": "",
                "BoundingBoxes": [st_clair_river_bbox], 
                "FilterMessageTypes": ["PositionReport"] 
            }

            await websocket.send(json.dumps(subscribe_message))
            print(f"[{datetime.now(timezone.utc)}] Subscription sent for St. Clair River delta region.")
            print(f"[{datetime.now(timezone.utc)}] Monitoring 11 designated Zones...")

            # Listen loop
            async for message_json in websocket:
                message = json.loads(message_json)
                message_type = message.get("MessageType")

                if message_type == "PositionReport":
                    # Extract Ship Name from Metadata wrapper
                    metadata = message.get("MetaData", {})
                    ship_name = metadata.get("ShipName", "UNKNOWN").strip()
                    
                    # Extract coordinates from the report
                    position_report = message['Message']['PositionReport']
                    latitude = position_report.get('Latitude')
                    
                    if latitude:
                        zone = get_zone_number(latitude)
                        
                        if zone:
                            print(f"[{datetime.now(timezone.utc)}] {ship_name} is in Zone {zone}")
                        else:
                            print(f"[{datetime.now(timezone.utc)}] {ship_name} detected slightly outside zone boundaries (Lat: {latitude})")
                            
                else:
                    print(f"[{datetime.now(timezone.utc)}] Received message type: {message_type}")

    except Exception as e:
        print(f"Connection error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())
