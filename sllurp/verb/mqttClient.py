# python 3.6
import random
import time
import json
import ast
from paho.mqtt import client as mqtt_client

StationID = 1
topic = "Station_" + str(StationID)
broker = 'ec2-13-38-177-0.eu-west-3.compute.amazonaws.com'
port = 1883
RollCallTopic = "RollCall"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'fintes'
password = 'fintes'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port)
    return client

def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    print("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg
    if msg.topic == "RollCall" :
        print("devices : " + str(msg.payload))
        message = str(msg.payload)
        result = ast.literal_eval(message)
        hop = json.loads(result)
        print(hop["StationID"])
    elif msg.topic == "cow_test" :
        print("Cow RFID  : " + str(msg.payload))
    else :
        print("Wrong Topic")


def publish(client):
    msg_count = 0
    while True:
        time.sleep(1)
        msg = f"messages: {msg_count}"
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1

def publishRollCall(client):
    jsonStation = {}
    jsonStation["StationID"] = StationID
    jsonStation["AntennaCnt"] = 4
    jsonStation["Timestamp"] = time.time()
    jsonStation["Longitude"] = 40
    jsonStation["Latitude"] = 40
    jsonStation["Enable"] = 1
    jsonStation["IP"] = "192.168.0.1"
    jsonStation["Version"] = "1"
    jsonStation["ReaderIP"] = "192.168.1.1"
    jsonStation["ReaderStatus"] = 1
    jsonStation["Active"] = 1
    
    result = client.publish(RollCallTopic, json.dumps(jsonStation))
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{jsonStation}` to topic `{RollCallTopic}`")
    else:
        print(f"Failed to send message to topic {RollCallTopic}")

def run():
    client = connect_mqtt()
    client.subscribe(RollCallTopic) 
    client.subscribe(topic)  
    client.loop_start()
    publishRollCall(client)
    while True:
        time.sleep(1)
        publish(client)


if __name__ == '__main__':
    run()