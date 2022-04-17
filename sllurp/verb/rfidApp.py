from sllurp import llrp
from twisted.internet import reactor, defer
import sqlite3
import sqliteTest as sq
import time
import json
import ast
from paho.mqtt import client as mqtt_client
import netifaces as ni
import signal, sys
import threading
import logging
import os
import pprint
from uptime import uptime

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = "logfile.log",
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.INFO)

stationObj = {}
logger = logging.getLogger()

uptimePi = 0
version = "1"
imei = ""
signalQuality = 0.0
StationID = 1
topic = "Station_" + str(StationID)
broker = 'ec2-13-38-177-0.eu-west-3.compute.amazonaws.com'
port = 1883
RollCallTopic = "RollCallVM"
rollCallAckTopic = "RollCallReader"
rebootTopic = "reboot"
restartTopic = "appRestart"
configTopic = "stationConfig"
client = {}
readerActive = 1

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-' + str(StationID)
username = 'fintes'
password = 'fintes'

READER_IP_ADDRESS = '169.254.194.84'
SCAN_TIME = 20
conn = sqlite3.connect('test.db')


def ping():
    hostname = READER_IP_ADDRESS #example
    response = os.system("ping -c 1 " + hostname)

    #and then check the response...
    if response == 0:
        logger.info("is up")
        readerActive = 1
    else:
        readerActive = 0
        logger.error("is down")
        reconnect()

bRun = 1
def sigint_handler(signal, frame):
	logger.info("Keyboard interrupt caught")
	reconnect()

connected = 0
def connect_mqtt():
    global client
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            global connected
            logger.info("Connected to MQTT Broker!")
            connected = 1
        else:
            logger.error("Failed to connect, return code %d\n", rc)
            connected = 0

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port, keepalive=20)
    return client

def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    logger.info("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg
    if msg.topic == RollCallTopic :
        logger.info("rollcall")
        publishRollCall(client)
    elif msg.topic == "cow_test" :
        logger.info("Cow RFID  : " + str(msg.payload))
    elif msg.topic == rebootTopic:
        message = str(msg.payload)
        result = ast.literal_eval(message)
        logger.info(result)
        if is_json(result) == True :            
            hop = json.loads(result)
            logger.info(hop)
            if hop["StationID"] == stationObj["StationID"] :
                os.system("sudo reboot -f")
    elif msg.topic == restartTopic:
        message = str(msg.payload)
        result = ast.literal_eval(message)
        logger.info(result)
        if is_json(result) == True :            
            hop = json.loads(result)
            logger.info(hop)
            if hop["StationID"] == stationObj["StationID"] :
                reconnect()     
    elif msg.topic == configTopic:
        message = str(msg.payload)
        result = ast.literal_eval(message)
        logger.info(result)
        if is_json(result) == True :            
            hop = json.loads(result)
            logger.info(hop)
            if hop["StationID"] == stationObj["StationID"] :
                logger.info("Change Config")
                with open('stationConfig.json', 'w') as outfile:
                    json.dump(hop, outfile)
                reconnect()
    else :
        logger.error("Wrong Topic " + msg.topic)

def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True 

def getLocalIP() :
    try:
        ni.ifaddresses('eth0')
        ip = ni.ifaddresses('eth0')[ni.AF_INET][0]['addr']
    except :
        return None
    return ip

def getWlanIP() :
    try:
        ni.ifaddresses('wlan0')
        wlanIp = ni.ifaddresses('wlan0')[ni.AF_INET][0]['addr']
    except :
        return None
    return wlanIp

def getGSMIP() :
    try:
        ni.ifaddresses('wwan0')
        gsmIP = ni.ifaddresses('wwan0')[ni.AF_INET][0]['addr']
    except :
        return None
    return gsmIP

def publishRollCall(client):    
    global signalQuality
    global stationObj
    global uptimePi
    stream = os.popen('/home/pi/.local/bin/atcom AT+CSQ')
    output = stream.readlines()
    signalQuality = float(output[2].split(": ",1)[1].split("\n")[0].replace(',','.'))
    logger.info(signalQuality)
    time.sleep(1)
    uptimePi = uptime()

    jsonStation = {}
    jsonStation["StationID"] = stationObj["StationID"]
    jsonStation["AntennaCnt"] = stationObj["AntennaCnt"]
    jsonStation["Timestamp"] = time.time()
    jsonStation["Longitude"] = stationObj["Longitude"]
    jsonStation["Latitude"] = stationObj["Latitude"]
    jsonStation["Enable"] = stationObj["Enable"]
    jsonStation["IP"] = getGSMIP()
    jsonStation["LocalIP"] = getLocalIP()
    jsonStation["Version"] = version
    jsonStation["ReaderIP"] = stationObj["ReaderIP"]
    jsonStation["ReaderStatus"] = readerActive
    jsonStation["Active"] = 1
    jsonStation["IMEI"] = imei
    jsonStation["signalQuality"] = signalQuality
    jsonStation["sshPort"] = stationObj["sshPort"]
    jsonStation["uptime"] = uptimePi
    jsonSt = json.dumps(jsonStation) 
    result = client.publish(rollCallAckTopic, jsonSt)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        logger.info(f"Send `{jsonSt}` to topic `{rollCallAckTopic}`")
    else:
        logger.error(f"Failed to send message to topic {rollCallAckTopic}")


def tagreportcb(llrp_msg):
    tags = llrp_msg.msgdict['RO_ACCESS_REPORT']['TagReportData']
    if len(tags):
        #logger.info(len(tags))
        #logger.info('saw tag(s): %s', pprint.pformat(tags))
        logger.info("numberOfTag : " + str(len(tags)))
        for tag in tags:
            TagID = tag['EPC-96']
            RoSpec = tag['ROSpecID'][0]
            Antenna = tag['AntennaID'][0]
            rssi = tag['PeakRSSI'][0]
            timestamp = tag['LastSeenTimestampUTC'][0]
            tagCount = tag['TagSeenCount'][0]
            #logger.info(TagID)
            logger.info("TagID :" + str(TagID) + "AntennaID : " + str(Antenna))
            sq.insertOrReplaceData(conn, TagID, Antenna, timestamp, rssi, RoSpec, tagCount)
    else:
        logger.info('no tags seen')

def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True

def sendData(client):
    logger.info("Send Data")
    jsonSend = sq.selectAndPrintAllData(conn)
    result = client.publish(topic, jsonSend)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        logger.info(f"Send `{jsonSend}` to topic `{topic}`")
        jsonSend = sq.deleteAllData(conn)
    else:
        logger.error(f"Failed to send message to topic {topic}")
    reactor.callLater(SCAN_TIME, sendData, client) 
    return result       

def report(llrp_msg):
    tag_list = tagreportcb(llrp_msg)

def finish(*args):
    logger.info("finish")
    if reactor.running:
        reactor.stop()

def shutdown(factory):
    logger.info("Shutdown triggered")
    return factory.politeShutdown()

def mqttInit():
    try :
        client = connect_mqtt()
        client.subscribe(RollCallTopic)
        client.subscribe(rebootTopic)
        client.subscribe(restartTopic)
        client.subscribe(configTopic)
	    #client.subscribe(rollCallAckTopic)  
        client.loop_start()
        publishRollCall(client)
        return client
    except :
        logger.error("MQTT ERROR")

def mqttLoop(client):
    while bRun :
        time.sleep(10)
        logger.info("MQTT loop")
        ping()

        if connected == 0:
            logger.error("MQTT Not Conencted")
            client = mqttInit()
            time.sleep(20)
        else:
            logger.info("MQTT Connected try to send")
            # try : 
            # 	result = client.publish("hop", "hoba")
            # 	#result = sendData(client)
            # 	status = result[0]
            # 	if status == 0:
	           #      print("Send to topic")
            # 	else:
	           #      print("Failed to send ")
            # except :
            #  	print("client publish failed")
def reconnect():
    global bRun
    logger.info("connection disconnected")
    if reactor.running:
        reactor.stop() 
    bRun = 0

def main():
    try :
        global bRun
        global client
        global imei
        global signalQuality
        global stationObj
        global uptimePi
        uptimePi = uptime()
        print(uptimePi)
        with open('stationConfig.json') as json_file:
            stationObj = json.load(json_file)
            logger.info(stationObj)
        stream = os.popen('/home/pi/.local/bin/atcom AT+CGSN')
        output = stream.readlines()
        imei = output[2].split("\n")[0]
        logger.info(imei)
        time.sleep(1)
        stream = os.system('/home/pi/.local/bin/atcom AT+CGSN > IMEI.txt')
        #time.sleep(60)
        signal.signal(signal.SIGINT, sigint_handler)
        sq.connectDatabase(conn)

        client = mqttInit()
        x = threading.Thread(target=mqttLoop, args=(client,))
        x.start()

        d = defer.Deferred()
        d.addCallback(reconnect)
        antmap = {stationObj["ReaderIP"]: {'1': 'ant1','2':'ant2','3':'ant3','4':'ant4'}}
        logger.info(antmap)

        factory_args = dict(
            start_first=True,
            #report_every_n_tags = 1,
            duration=5,
            antenna_dict=antmap,
            tx_power=0,
            session=0,
            tag_population=100,
            start_inventory=True,
            disconnect_when_done=False,
            reconnect=True,
            tag_content_selector={
                'EnableROSpecID': True,
                'EnableSpecIndex': False,
                'EnableInventoryParameterSpecID': False,
                'EnableAntennaID': True,
                'EnableChannelIndex': False,
                'EnablePeakRSSI': True,
                'EnableFirstSeenTimestamp': False,
                'EnableLastSeenTimestamp': True,
                'EnableTagSeenCount': True,
                'EnableAccessSpecID': False
            }
        )
        factory = llrp.LLRPClientFactory(**factory_args)
        factory.addTagReportCallback(report)
        tcpTest = reactor.connectTCP(stationObj["ReaderIP"], llrp.LLRP_PORT, factory, timeout=60)
        reactor.addSystemEventTrigger('before', 'shutdown', reconnect)

        # https://twistedmatrix.com/documents/current/core/howto/time.html
        reactor.callLater(SCAN_TIME, sendData, client)
        reactor.run()
        logger.info("Reactor Exited")
        bRun = 0
        factory.politeShutdown()
        x.join()
        # while bRun :
        #     time.sleep(20)
        #     d = defer.Deferred()
        #     d.addCallback(finish)
        #     factory = llrp.LLRPClientFactory(onFinish=d, antennas=[0], duration=0.5, session=0)
        #     factory.addTagReportCallback(report)
        #     reactor.connectTCP(READER_IP_ADDRESS, llrp.LLRP_PORT, factory, timeout=3)
        #     reactor.addSystemEventTrigger('before', 'shutdown', shutdown, factory)

        #     # https://twistedmatrix.com/documents/current/core/howto/time.html
        #     reactor.callLater(SCAN_TIME, sendData, client)
        #     reactor.run()
            # if connected == 1:
	        #     try : 
	        #     	#result = client.publish("hop", "hoba")
	        #     	result = sendData(client)
	        #     	status = result[0]
	        #     	if status == 0:
		    #             print("Send to topic X")
	        #     	else:
		    #             print("Failed to send  X")
	        #     except :
	        #      	print("client publish failed X")

    except:
        bRun = 0
        shutdown(factory)
        x.join()
        sys.exit()

if __name__ == "__main__":
    main()
