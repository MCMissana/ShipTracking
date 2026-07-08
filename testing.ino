// Verify that your arduino board can recieve data from aisstream
// bounding box set to high traffic area so you are guarenteed to get data back

#include <WiFi.h>
#include <WebSocketsClient.h>
#include <ArduinoJson.h>

// --- Credentials ---
const char* ssid = "";
const char* password = "";
const char* apiKey = "";

// AISStream WebSocket Details
const char* ws_host = "stream.aisstream.io";
const int ws_port = 443;
const char* ws_url = "/v0/stream";

// --- Pin Definitions ---
const int LIGHT_1 = 2;
const int LIGHT_2 = 3;
const int LIGHT_3 = 4;
const int LIGHT_4 = 5;

WebSocketsClient webSocket;

bool dataReceived = false;
bool disconnected = false;
bool readyToSendSubscription = false;
unsigned long connectionTime = 0;
const long interval = 500;

void handleAisMessage(uint8_t * payload, size_t length) {
    // payload from WStype_BIN is NOT null-terminated, so parse by length directly.
    JsonDocument doc; // ArduinoJson v7 style
    DeserializationError err = deserializeJson(doc, payload, length);

    if (err) {
        Serial.print("[JSON] Parse failed: ");
        Serial.println(err.c_str());
        return;
    }

    const char* messageType = doc["MessageType"] | "unknown";
    Serial.print("[AIS] MessageType: ");
    Serial.println(messageType);

    // Common metadata present on most message types
    if (doc["MetaData"].is<JsonObject>()) {
        JsonObject meta = doc["MetaData"];
        const char* shipName = meta["ShipName"] | "N/A";
        long mmsi = meta["MMSI"] | 0;
        double lat = meta["latitude"] | 0.0;
        double lon = meta["longitude"] | 0.0;
        const char* timeUtc = meta["time_utc"] | "";

        Serial.printf("  MMSI: %ld  Ship: %s\n", mmsi, shipName);
        Serial.printf("  Lat: %.5f  Lon: %.5f  Time: %s\n", lat, lon, timeUtc);
    }

    // Example: pull specific fields out of a PositionReport
    if (strcmp(messageType, "PositionReport") == 0) {
        JsonObject pr = doc["Message"]["PositionReport"];
        double sog = pr["Sog"] | 0.0;   // speed over ground
        double cog = pr["Cog"] | 0.0;   // course over ground
        Serial.printf("  SOG: %.1f kn  COG: %.1f deg\n", sog, cog);
    }
}

void webSocketEvent(WStype_t type, uint8_t * payload, size_t length) {
    switch (type) {
        case WStype_DISCONNECTED:
            Serial.println("[EVENT] WebSocket Disconnected.");
            if (dataReceived && !disconnected) {
                disconnected = true;
                digitalWrite(LIGHT_4, HIGH);
            }
            break;

        case WStype_CONNECTED:
            Serial.println("\n[EVENT] WebSocket Connection Opened!");
            digitalWrite(LIGHT_2, HIGH);
            connectionTime = millis();
            readyToSendSubscription = true;
            break;

        case WStype_TEXT:
        case WStype_BIN:  // AISStream sends data as binary frames
            if (!dataReceived) {
                dataReceived = true;
                Serial.println("\n[EVENT] >>> Data Received!");
                digitalWrite(LIGHT_3, HIGH);
            }
            handleAisMessage(payload, length);
            break;

        case WStype_ERROR:
            Serial.println("[ERROR] WebSocket Error!");
            break;

        default:
            // fragments, pings, pongs etc. -- ignore or log if you want
            break;
    }
}

void setup() {
    Serial.begin(115200);
    for (int i = 2; i <= 12; i++) { pinMode(i, OUTPUT); digitalWrite(i, LOW); }

    WiFi.begin(ssid, password);
    while (WiFi.status() != WL_CONNECTED) { delay(500); Serial.print("."); }

    digitalWrite(LIGHT_1, HIGH);
    webSocket.beginSSL(ws_host, ws_port, ws_url, "", "wss");
    webSocket.onEvent(webSocketEvent);
}

void loop() {
    webSocket.loop();

    if (readyToSendSubscription && (millis() - connectionTime >= interval)) {
        readyToSendSubscription = false;
        String subMsg = "{\"APIKey\":\"" + String(apiKey) + "\",\"BoundingBoxes\":[[[-90,-180],[90,180]]],\"FilterMessageTypes\":[\"PositionReport\"]}";
        Serial.println("[API] Sending subscription...");
        if (webSocket.sendTXT(subMsg)) {
            Serial.println("[API] TX SUCCESS");
        }
    }
}
