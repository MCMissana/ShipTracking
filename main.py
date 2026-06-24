import asyncio
import websockets
import json
import ssl
from datetime import datetime, timezone

async def connect_ais_stream():
    uri = "wss://stream.aisstream.io/v0/stream"
    
    # Create an explicit SSL context to prevent handshake delays
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    print(f"[{datetime.now(timezone.utc)}] Connecting to AISStream...")
    
    try:
        async with websockets.connect(uri, ssl=ssl_context) as websocket:
            # AISStream API requires: [[LatitudeStart, LongitudeStart], [LatitudeEnd, LongitudeEnd]]
            # Extracted from your areaPolygon data:
            # Latitudes: 42.57846 to 42.71884
            # Longitudes: -82.562256 to -82.42218
            st_clair_river_bbox = [[42.57846, -82.562256], [42.71884, -82.42218]]

            # Construct a fully compliant subscription payload with your specific bounds
            subscribe_message = {
                "APIKey": "",
                "BoundingBoxes": [st_clair_river_bbox], 
                "FilterMessageTypes": ["PositionReport"]
            }

            # Send instantly to beat the 3-second server timeout rule
            await websocket.send(json.dumps(subscribe_message))
            print(f"[{datetime.now(timezone.utc)}] Subscription sent successfully for St. Clair River delta region.")
            print(f"[{datetime.now(timezone.utc)}] Awaiting data stream...")

            # Listen loop
            async for message_json in websocket:
                message = json.loads(message_json)
                message_type = message.get("MessageType")

                if message_type == "PositionReport":
                    ais_message = message['Message']['PositionReport']
                    print(f"[{datetime.now(timezone.utc)}] TARGET FOUND! -> ShipID: {ais_message['UserID']} | Lat: {ais_message['Latitude']} | Lon: {ais_message['Longitude']}")
                    
                    # Because the AISStream backend filters the stream to only send ships inside your bounding box,
                    # any PositionReport hitting this block is guaranteed to be within your specified area.
                    # TODO: Trigger physical light/notification logic here.
                    
                else:
                    # Print anything else the server sends back (like errors or metadata)
                    print(f"[{datetime.now(timezone.utc)}] Received other message type: {message_type}")

    except Exception as e:
        print(f"Connection error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())
