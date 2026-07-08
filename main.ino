#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <WebSocketsClient.h> 
#include <ArduinoJson.h>
#include <time.h>             

// --- Wi-Fi Credentials ---
const char* ssid = "";
const char* password = "";

// --- Core Zone Configuration ---
const double TRACKING_LAT_MIN = 42.57846;
const double TRACKING_LAT_MAX = 42.71884;
const double LON_MIN = -82.562256;
const double LON_MAX = -82.42218;

const double TOTAL_LAT_SPAN = TRACKING_LAT_MAX - TRACKING_LAT_MIN;
const double ZONE_HEIGHT = TOTAL_LAT_SPAN / 11.0;

const double AIS_LAT_MIN = TRACKING_LAT_MIN - 0.02;
const double AIS_LAT_MAX = TRACKING_LAT_MAX + 0.02;

const double MIN_TRACKING_SPEED_KNOTS = 4.0;
const unsigned long MAX_DEAD_RECKON_MILLIS = 45 * 60 * 1000; 

// --- Error States ---
enum ErrorType {
  ERR_NONE = 0,
  ERR_WIFI = 2,       
  ERR_WEBSOCKET = 3,  
  ERR_JSON = 4        
};
ErrorType currentError = ERR_NONE;

unsigned long lastFlashTime = 0;
bool flashState = false;

// --- Global Sync Tracking Flags ---
bool productionSubSent = false; 

// --- Ship Struct definition ---
struct Ship {
  uint32_t mmsi = 0;
  char name[30] = "";
  char direction[6] = "Unk";
  double latitude = 0.0;
  double sog = 0.0;
  unsigned long lastRealApiUpdate = 0;
};

const int MAX_SHIPS_PER_ZONE = 3; 
Ship zones[12][MAX_SHIPS_PER_ZONE]; 

WebSocketsClient webSocket;
unsigned long lastDeadReckonTime = 0;

// --- Helper Functions ---
void clearAllPins() {
  for (int pin = 2; pin <= 12; pin++) {
    digitalWrite(pin, LOW);
  }
}

void blinkAllLights(int count) {
  for (int i = 0; i < count; i++) {
    for (int pin = 2; pin <= 12; pin++) {
      digitalWrite(pin, HIGH);
    }
    delay(250);
    clearAllPins();
    delay(250);
  }
}

int getZoneNumber(double latitude) {
  if (latitude > TRACKING_LAT_MAX && latitude <= AIS_LAT_MAX) return -1; 
  if (latitude >= AIS_LAT_MIN && latitude < TRACKING_LAT_MIN) return -2;  
  
  if (latitude >= TRACKING_LAT_MIN && latitude <= TRACKING_LAT_MAX) {
    double distanceFromTop = TRACKING_LAT_MAX - latitude;
    int zoneIndex = (int)(distanceFromTop / ZONE_HEIGHT);
    int zoneNumber = zoneIndex + 1;
    if (zoneNumber > 11) zoneNumber = 11;
    return zoneNumber;
  }
  return 0; 
}

const char* getDirection(int cog) {
  if (cog > 90 && cog <= 270) return "South";
  return "North";
}

void updateZonePins() {
  for (int zone = 1; zone <= 11; zone++) {
    int targetPin = zone + 1; 
    if (currentError != ERR_NONE && targetPin == (int)currentError) continue; 

    bool zoneActive = false;
    for (int s = 0; s < MAX_SHIPS_PER_ZONE; s++) {
      if (zones[zone][s].mmsi != 0) {
        zoneActive = true;
        break;
      }
    }
    digitalWrite(targetPin, zoneActive ? HIGH : LOW);
  }
}

void removeShip(uint32_t mmsi, const char* name, const char* reason) {
  const char* safeName = (name && strlen(name) > 0) ? name : "Unknown";
  for (int z = 1; z <= 11; z++) {
    for (int s = 0; s < MAX_SHIPS_PER_ZONE; s++) {
      if (zones[z][s].mmsi == mmsi) {
        zones[z][s].mmsi = 0;
        Serial.printf("[%lu] %s (MMSI: %u) REMOVED from Zone %d via %s.\n", millis(), safeName, mmsi, z, reason);
      }
    }
  }
  updateZonePins();
}

void updateShip(uint32_t mmsi, const char* name, int zoneNum, const char* direction, double currentLat, double sog) {
  const char* safeName = (name && strlen(name) > 0) ? name : "Unknown";
  
  for (int z = 1; z <= 11; z++) {
    if (z != zoneNum) {
      for (int s = 0; s < MAX_SHIPS_PER_ZONE; s++) {
        if (zones[z][s].mmsi == mmsi) zones[z][s].mmsi = 0;
      }
    }
  }

  double validSog = (sog < 102.2) ? sog : 6.0;
  bool updated = false;

  for (int s = 0; s < MAX_SHIPS_PER_ZONE; s++) {
    if (zones[zoneNum][s].mmsi == mmsi) {
      zones[zoneNum][s].latitude = currentLat;
      zones[zoneNum][s].sog = validSog;
      strncpy(zones[zoneNum][s].direction, direction, sizeof(zones[zoneNum][s].direction) - 1);
      zones[zoneNum][s].direction[sizeof(zones[zoneNum][s].direction) - 1] = '\0';
      zones[zoneNum][s].lastRealApiUpdate = millis();
      updated = true;
      break;
    }
  }

  if (!updated) {
    for (int s = 0; s < MAX_SHIPS_PER_ZONE; s++) {
      if (zones[zoneNum][s].mmsi == 0) {
        zones[zoneNum][s].mmsi = mmsi;
        strncpy(zones[zoneNum][s].name, safeName, sizeof(zones[zoneNum][s].name) - 1);
        zones[zoneNum][s].name[sizeof(zones[zoneNum][s].name) - 1] = '\0';
        strncpy(zones[zoneNum][s].direction, direction, sizeof(zones[zoneNum][s].direction) - 1);
        zones[zoneNum][s].direction[sizeof(zones[zoneNum][s].direction) - 1] = '\0';
        zones[zoneNum][s].latitude = currentLat;
        zones[zoneNum][s].sog = validSog;
        zones[zoneNum][s].lastRealApiUpdate = millis();
        Serial.printf("[%lu] %s entered Zone %d\n", millis(), safeName, zoneNum);
        break;
      }
    }
  }
  updateZonePins();
}

void processDeadReckoning() {
  unsigned long now = millis();
  for (int z = 1; z <= 11; z++) {
    for (int s = 0; s < MAX_SHIPS_PER_ZONE; s++) {
      if (zones[z][s].mmsi == 0) continue;
      Serial.println("sim");
      Serial.println(zones[z][s].mmsi);

      if (now - zones[z][s].lastRealApiUpdate > MAX_DEAD_RECKON_MILLIS) {
        removeShip(zones[z][s].mmsi, zones[z][s].name, "Absolute Timeout");
        continue;
      }

      double shipLatChangePerMinute = zones[z][s].sog / 3600.0;
      if (strcmp(zones[z][s].direction, "North") == 0) zones[z][s].latitude += shipLatChangePerMinute;
      else if (strcmp(zones[z][s].direction, "South") == 0) zones[z][s].latitude -= shipLatChangePerMinute;

      int newZone = getZoneNumber(zones[z][s].latitude);
      if (newZone == -1 || newZone == -2 || newZone == 0) {
        removeShip(zones[z][s].mmsi, zones[z][s].name, "Simulated Exit Box");
      } else if (newZone != z) {
        Ship migrationData = zones[z][s];
        zones[z][s].mmsi = 0; 
        for (int ns = 0; ns < MAX_SHIPS_PER_ZONE; ns++) {
          if (zones[newZone][ns].mmsi == 0) {
            zones[newZone][ns] = migrationData;
            break;
          }
        }
      }
    }
  }
  updateZonePins();
}

// --- Subscription Payload Generators ---
String getSubscriptionPayload() {
  StaticJsonDocument<400> subDoc;
  subDoc["APIKey"] = ""; 
  
  JsonArray boundingBoxes = subDoc.createNestedArray("BoundingBoxes");
  JsonArray box1 = boundingBoxes.createNestedArray();
  JsonArray coord1 = box1.createNestedArray();
  
  // Uses 'serialized' to prevent ArduinoJson from truncation optimization
  coord1.add(serialized(String(AIS_LAT_MIN, 6))); 
  coord1.add(serialized(String(LON_MIN, 6)));
  
  JsonArray coord2 = box1.createNestedArray();
  coord2.add(serialized(String(AIS_LAT_MAX, 6))); 
  coord2.add(serialized(String(LON_MAX, 6)));

  JsonArray filterMsg = subDoc.createNestedArray("FilterMessageTypes");
  filterMsg.add("PositionReport");

  String msg;
  serializeJson(subDoc, msg);
  return msg;
}

// --- Dynamic Event Handler ---
void webSocketEvent(WStype_t type, uint8_t * payload, size_t length) {
  switch(type) {
    case WStype_DISCONNECTED:
      Serial.println("[WS] Disconnected!");
      currentError = ERR_WEBSOCKET;
      productionSubSent = false; 
      break;

    case WStype_CONNECTED:
      Serial.println("[WS] Connected to Server Cleanly!");
      currentError = ERR_NONE;
      break;

    case WStype_TEXT:
      {
        // Add this before anything else — see exactly what the server sends
        Serial.printf("[WS] RX %u bytes: %s\n", (unsigned)length, (const char*)payload);


        DynamicJsonDocument doc(4096);
        DeserializationError error = deserializeJson(doc, payload, length);

        if (error) {
          Serial.print("JSON Deserialization Error: ");
          Serial.println(error.c_str());
          currentError = ERR_JSON;
          return;
        }

        if (currentError == ERR_JSON) {
          currentError = ERR_NONE;
          updateZonePins();
        }

        const char* messageType = doc["MessageType"];
        if (messageType && strcmp(messageType, "PositionReport") == 0) {
          JsonObject metadata = doc["MetaData"];
          
          const char* shipName = metadata["ShipName"];
          if (!shipName) {
            shipName = "Unknown";
          }
          
          uint32_t mmsi = metadata["MMSI"];

          JsonObject posReport = doc["Message"]["PositionReport"];
          if (posReport.isNull()) return;

          double latitude = posReport["Latitude"];
          int cog = posReport["Cog"];
          double sog = posReport["Sog"];

          if (latitude != 0 && mmsi != 0) {
            if (sog < MIN_TRACKING_SPEED_KNOTS) {
              removeShip(mmsi, shipName, "Speed under threshold");
              return;
            }

            int zone = getZoneNumber(latitude);
            if (zone == -1 || zone == -2) {
              removeShip(mmsi, shipName, "Hit exit boundary");
            } else if (zone > 0) {
              const char* dir = getDirection(cog);
              updateShip(mmsi, shipName, zone, dir, latitude, sog);
            }
          }
        }
      }
      break;

    case WStype_BIN:
      {
        // Add this before anything else — see exactly what the server sends
        Serial.printf("[WS] RX %u bytes: %s\n", (unsigned)length, (const char*)payload);


        JsonDocument doc;
        DeserializationError error = deserializeJson(doc, payload, length);

        if (error) {
          Serial.print("JSON Deserialization Error: ");
          Serial.println(error.c_str());
          currentError = ERR_JSON;
          return;
        }

        if (currentError == ERR_JSON) {
          currentError = ERR_NONE;
          updateZonePins();
        }

        const char* messageType = doc["MessageType"];
        if (messageType && strcmp(messageType, "PositionReport") == 0) {
          JsonObject metadata = doc["MetaData"];
          
          const char* shipName = metadata["ShipName"];
          if (!shipName) {
            shipName = "Unknown";
          }
          
          uint32_t mmsi = metadata["MMSI"];

          JsonObject posReport = doc["Message"]["PositionReport"];
          if (posReport.isNull()) return;

          double latitude = posReport["Latitude"].as<double>();
          int cog = posReport["Cog"];
          double sog = posReport["Sog"];

          if (latitude != 0 && mmsi != 0) {
            if (sog < MIN_TRACKING_SPEED_KNOTS) {
              removeShip(mmsi, shipName, "Speed under threshold");
              return;
            }

            int zone = getZoneNumber(latitude);
            if (zone == -1 || zone == -2) {
              removeShip(mmsi, shipName, "Hit exit boundary");
            } else if (zone > 0) {
              const char* dir = getDirection(cog);
              updateShip(mmsi, shipName, zone, dir, latitude, sog);
            }
          }
        }
      }
      break;
      
    case WStype_ERROR:
      Serial.println("[WS] Socket error encountered.");
      currentError = ERR_WEBSOCKET;
      break;
  }
}

// --- Arduino Core Initialization ---
void setup() {
  Serial.begin(115200);
  delay(1000);


  for (int pin = 2; pin <= 12; pin++) {
    pinMode(pin, OUTPUT);
    digitalWrite(pin, LOW);
  }

  Serial.printf("Connecting to Wi-Fi: %s\n", ssid);
  WiFi.mode(WIFI_STA); 
  WiFi.begin(ssid, password);
  
  unsigned long startWifiTime = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - startWifiTime < 15000) {
    delay(500);
    Serial.print(".");
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\nWi-Fi Connected successfully.");
    
    Serial.println("Syncing time via NTP... (Required for Secure TLS validation)");
    configTime(0, 0, "pool.ntp.org", "time.nist.gov");
    time_t now = time(nullptr);
    while (now < 8 * 3600 * 2) {
      delay(500);
      Serial.print(".");
      now = time(nullptr);
    }
    Serial.println("\nTime synced successfully!");
    
    blinkAllLights(2); 
    
    // --- Initialize WebSockets Directly ---
    Serial.println("Initializing final stream tracking routine...");
    webSocket.beginSSL("stream.aisstream.io", 443, "/v0/stream");
    webSocket.setExtraHeaders("User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)\r\nOrigin: https://stream.aisstream.io");
    webSocket.onEvent(webSocketEvent); 
    webSocket.setReconnectInterval(5000); 

  } else {
    Serial.println("\nWi-Fi Setup timed out.");
    currentError = ERR_WIFI;
  }
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    currentError = ERR_WIFI;
    productionSubSent = false; 
  } else {
    if (currentError == ERR_WIFI) {
      currentError = ERR_WEBSOCKET; 
      clearAllPins();
    }
    
    webSocket.loop(); 

    if (webSocket.isConnected() && !productionSubSent) {
      Serial.println("[Production] Line verified. Transmitting tracking box parameters...");
      String subPayload = getSubscriptionPayload();
      if (webSocket.sendTXT(subPayload)) {
        Serial.println("[Production] Global Tracking criteria running.");
        productionSubSent = true;
      }
    }
  }

  // --- Non-blocking Error LED Flash Control Loop ---
  if (currentError != ERR_NONE) {
    if (millis() - lastFlashTime >= 300) { 
      lastFlashTime = millis();
      flashState = !flashState;
      
      digitalWrite(2, LOW);
      digitalWrite(3, LOW);
      digitalWrite(4, LOW);
      
      digitalWrite((int)currentError, flashState ? HIGH : LOW);
    }
  }

  if (millis() - lastDeadReckonTime >= 60000) {
    processDeadReckoning();
    lastDeadReckonTime = millis();
  }
}
