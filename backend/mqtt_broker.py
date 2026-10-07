import json
import random
import time
from datetime import datetime

import paho.mqtt.client as mqtt


# MQTT broker
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883

# Use a unique topic for our test
MQTT_TOPIC = "siri-nizampatnam/ai-agent/sensors"

# Create MQTT client
client = mqtt.Client()

print("Connecting to MQTT broker...")
client.connect(MQTT_BROKER, MQTT_PORT, 60)

print("Connected successfully!")
print(f"Publishing to: {MQTT_TOPIC}")
print("Press Ctrl+C to stop.\n")


try:
    while True:

        # Generate fake sensor values
        sensor_data = {
            "timestamp": datetime.now().isoformat(),
            "Temp1": round(random.uniform(70, 80), 2),
            "Temp2": round(random.uniform(72, 82), 2),
            "Temp3": round(random.uniform(75, 85), 2),
            "Temp4": round(random.uniform(78, 88), 2),
            "Temp5": round(random.uniform(85, 95), 2),
            "Temp6": round(random.uniform(70, 80), 2),
            "Pressure1": round(random.uniform(100, 105), 2),
        }

        # Convert dictionary to JSON
        message = json.dumps(sensor_data)

        # Publish to MQTT
        result = client.publish(MQTT_TOPIC, message)

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"Published: {message}")
        else:
            print(f"Publish failed. Error code: {result.rc}")

        # Wait 2 seconds before next reading
        time.sleep(2)

except KeyboardInterrupt:
    print("\nSimulator stopped.")

finally:
    client.disconnect()
    print("Disconnected from MQTT broker.")