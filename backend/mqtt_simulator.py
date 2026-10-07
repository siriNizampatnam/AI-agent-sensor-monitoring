import json
import random
import time
from datetime import datetime

import paho.mqtt.client as mqtt


# ============================================================
# MQTT CONFIGURATION
# ============================================================

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "siri-nizampatnam/ai-agent/sensors"

# Public HiveMQ broker does not require username/password for this test.
MQTT_USERNAME = ""
MQTT_PASSWORD = ""

# Keep this False because the test broker/port above uses plain MQTT.
MQTT_USE_TLS = False

# Publish one new sensor reading every 2 seconds.
PUBLISH_INTERVAL = 2.0

# QoS 1 asks the broker to acknowledge the message.
MQTT_QOS = 1


# ============================================================
# CONNECTION STATE
# ============================================================

connected = False


def on_connect(client, userdata, flags, reason_code, properties=None):
    global connected

    if getattr(reason_code, "is_failure", False):
        connected = False
        print(f"[MQTT] Connection failed: {reason_code}")
        return

    connected = True
    print(f"[MQTT] Connected to {MQTT_BROKER}:{MQTT_PORT}")
    print(f"[MQTT] Publishing to: {MQTT_TOPIC}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    global connected

    connected = False
    print(f"[MQTT] Disconnected. Reason: {reason_code}")


# ============================================================
# CREATE MQTT CLIENT
# ============================================================

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="siri-sensor-simulator",
)

if MQTT_USERNAME:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

if MQTT_USE_TLS:
    client.tls_set()

client.on_connect = on_connect
client.on_disconnect = on_disconnect

# Automatically increase the reconnect delay when the connection
# is temporarily unavailable.
client.reconnect_delay_set(min_delay=1, max_delay=30)


# ============================================================
# GENERATE SENSOR DATA
# ============================================================

def create_sensor_data():
    """
    Generate a completely new random reading for every sensor
    at every timestamp.

    Temp3 intentionally ranges around 75 so a threshold such as
    Temp3 > 75 can naturally move between FALSE and TRUE.
    """

    return {
        "timestamp": datetime.now().isoformat(),

        "Temp1": round(random.uniform(70, 80), 2),
        "Temp2": round(random.uniform(72, 82), 2),

        # Intentionally crosses both sides of 75.
        "Temp3": round(random.uniform(70, 85), 2),

        "Temp4": round(random.uniform(78, 88), 2),
        "Temp5": round(random.uniform(85, 95), 2),
        "Temp6": round(random.uniform(70, 80), 2),
        "Pressure1": round(random.uniform(100, 105), 2),
    }


# ============================================================
# DISPLAY ALL SENSOR VALUES
# ============================================================

def display_sensor_data(sensor_data):
    print("=" * 80)
    print(f"Timestamp : {sensor_data['timestamp']}")
    print(f"Temp1     : {sensor_data['Temp1']:.2f}")
    print(f"Temp2     : {sensor_data['Temp2']:.2f}")
    print(f"Temp3     : {sensor_data['Temp3']:.2f}")
    print(f"Temp4     : {sensor_data['Temp4']:.2f}")
    print(f"Temp5     : {sensor_data['Temp5']:.2f}")
    print(f"Temp6     : {sensor_data['Temp6']:.2f}")
    print(f"Pressure1 : {sensor_data['Pressure1']:.2f}")
    print("=" * 80)


# ============================================================
# CONNECT AND START NETWORK LOOP
# ============================================================

print("Starting MQTT sensor simulator...")
print(f"Broker    : {MQTT_BROKER}:{MQTT_PORT}")
print(f"Topic     : {MQTT_TOPIC}")
print("All sensor values are randomly generated.")
print("Press Ctrl+C to stop.\n")

try:
    # The async connection allows the MQTT network loop to manage
    # connection/reconnection in the background.
    client.connect_async(
        MQTT_BROKER,
        MQTT_PORT,
        60,
    )

    client.loop_start()

    # Give the MQTT loop a few seconds to establish the first connection.
    for _ in range(100):
        if connected:
            break
        time.sleep(0.1)

    if not connected:
        print("[MQTT] Initial connection has not completed yet.")
        print("[MQTT] The client will keep trying to reconnect.")

    # ========================================================
    # CONTINUOUS PUBLISHING
    # ========================================================

    while True:
        sensor_data = create_sensor_data()
        message = json.dumps(sensor_data)

        if not connected:
            print("[MQTT] Not connected. Waiting for broker connection...")
            time.sleep(PUBLISH_INTERVAL)
            continue

        result = client.publish(
            MQTT_TOPIC,
            message,
            qos=MQTT_QOS,
            retain=False,
        )

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            display_sensor_data(sensor_data)
            print("[MQTT] Published successfully\n")
        else:
            print(f"[MQTT] Publish failed. Error code: {result.rc}\n")

        time.sleep(PUBLISH_INTERVAL)

except KeyboardInterrupt:
    print("\nSimulator stopped by user.")

except Exception as exc:
    print(f"\n[SIMULATOR ERROR] {type(exc).__name__}: {exc}")

finally:
    client.loop_stop()
    client.disconnect()
    print("Disconnected from MQTT broker.")
