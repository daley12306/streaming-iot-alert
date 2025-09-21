import json, time, random, datetime, uuid
import paho.mqtt.client as mqtt

BROKER="broker.hivemq.com"
PORT=1883
TOPIC="iot/prod/sensors"

ITEMS=[f"WIP-{i:06d}" for i in range(102000,102150)]
STATIONS=["ST-01","ST-02","ST-03","ST-04"]
STATES=["QUEUED","PROCESSING","INSPECTION","PACKED"]

cli = mqtt.Client(client_id=f"pub-mfg-{random.randint(1000,9999)}")
cli.connect(BROKER, PORT, 30)

try:
    while True:
        item=random.choice(ITEMS)
        stn=random.choice(STATIONS)
        state=random.choices(STATES, weights=[0.2,0.5,0.2,0.1])[0]
        defect = (state=="INSPECTION" and random.random()<0.3)
        msg={
        "item_id": item,
        "station_id": stn,
        "state": state,
        "defect": defect,
        "ts": datetime.datetime.now().isoformat(),
        "msg_id": str(uuid.uuid4())
        }

        """
        Gửi message dạng JSON lên MQTT
        """
        # BEGIN YOUR CODE
        ...
        # END YOUR CODE 

        print("PUB>", msg)
        time.sleep(0.5)
except KeyboardInterrupt:
    pass
finally:
    cli.disconnect()