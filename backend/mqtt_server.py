import json
import paho.mqtt.client as mqtt


# ============================================================
# MQTT CONNECTION DETAILS
# Replace these with the details provided by your mentor.
# ============================================================

MQTT_HOST = "YOUR_MQTT_HOST"
MQTT_PORT = 1883
MQTT_TOPIC = "YOUR_MQTT_TOPIC"

MQTT_USERNAME = "YOUR_USERNAME"
MQTT_PASSWORD = "YOUR_PASSWORD"

MQTT_USE_TLS = False


# ============================================================
# CONNECTED
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to MQTT broker")

        client.subscribe(MQTT_TOPIC)

        print(f"Subscribed to topic: {MQTT_TOPIC}")

    else:
        print(f"MQTT connection failed. Reason code: {reason_code}")


# ============================================================
# MESSAGE RECEIVED
# ============================================================

def on_message(client, userdata, message):

    try:
        payload = message.payload.decode("utf-8")

        print("\n---------------- MQTT MESSAGE ----------------")
        print(f"Topic: {message.topic}")
        print(f"Payload: {payload}")

        # Try to interpret the message as JSON
        try:
            data = json.loads(payload)

            print("Parsed JSON:")
            print(json.dumps(data, indent=2))

        except json.JSONDecodeError:
            print("Message is not JSON.")

        print("----------------------------------------------")

    except Exception as e:
        print(f"Error processing MQTT message: {e}")


# ============================================================
# CREATE MQTT CLIENT
# ============================================================

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2
)

client.on_connect = on_connect
client.on_message = on_message


# ============================================================
# AUTHENTICATION
# ============================================================

if MQTT_USERNAME and MQTT_PASSWORD:
    client.username_pw_set(
        MQTT_USERNAME,
        MQTT_PASSWORD
    )


# ============================================================
# TLS / SSL
# ============================================================

if MQTT_USE_TLS:
    client.tls_set()


# ============================================================
# CONNECT
# ============================================================

print("Connecting to MQTT broker...")

client.connect(
    MQTT_HOST,
    MQTT_PORT,
    keepalive=60
)


# ============================================================
# KEEP LISTENING
# ============================================================

print("Waiting for live MQTT data...")

client.loop_forever()