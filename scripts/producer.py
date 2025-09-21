import json
import paho.mqtt.client as mqtt, ssl, json
from kafka import KafkaProducer

"""
Dựa vào cấu hình ở generator.py để cấu hình các thông số kết nối dưới đây
"""
BROKER=...
PORT=...
TOPIC=...

KAFKA_TOPIC="sensors-telemetry"

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_msg(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        payload = {"raw": msg.payload.decode("utf-8", errors="ignore")}

    print("FORWARD >", payload)
    producer.send(KAFKA_TOPIC, value=payload)

m = mqtt.Client(client_id="bridge-1", protocol=mqtt.MQTTv311)
m.connect(BROKER, PORT, 30)
m.subscribe("iot/prod/sensors/#")
m.on_message = on_msg

m.loop_forever()