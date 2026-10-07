import json
import paho.mqtt.client as mqtt

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "siri-nizampatnam/ai-agent/sensors"


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected to MQTT broker. Reason code: {reason_code}")

    client.subscribe(MQTT_TOPIC)

    print(f"Subscribed to: {MQTT_TOPIC}")
    print("Waiting for live MQTT data...\n")


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))

        print("Received live data:")
        print(data)
        print("-" * 80)

    except Exception as e:
        print("Error reading MQTT message:", e)


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

client.on_connect = on_connect
client.on_message = on_message

print("Connecting to MQTT broker...")

client.connect(MQTT_BROKER, MQTT_PORT, 60)

client.loop_forever()