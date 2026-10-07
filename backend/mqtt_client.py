import json
import paho.mqtt.client as mqtt

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "siri-nizampatnam/ai-agent/sensors"


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"[MQTT] Connected to broker. Reason: {reason_code}")

    client.subscribe(MQTT_TOPIC)

    print(f"[MQTT] Subscribed to: {MQTT_TOPIC}")


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))

        print("[MQTT] Live data received:")
        print(data)

        # Send received data to FastAPI application
        if userdata and "callback" in userdata:
            userdata["callback"](data)

    except Exception as e:
        print(f"[MQTT] Error processing message: {e}")


def start_mqtt(callback):
    userdata = {
        "callback": callback
    }

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    client.user_data_set(userdata)

    client.on_connect = on_connect
    client.on_message = on_message

    print("[MQTT] Connecting...")

    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    client.loop_start()

    return client