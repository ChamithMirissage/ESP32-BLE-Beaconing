# Get data as this format : {"id" : "1","UoM_Wireless1" : "-58","eduroam1" : "-89","UoM_Wireless1" : "-89","eduroam1" : "-57"}
import paho.mqtt.client as mqtt
import json
import csv
import pandas as pd
import os
import ast

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("EN3250/ESP32")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    data_store_csv(msg.payload)
    

# Function to store received data in a csv file
def data_store_csv(data1):

    # Define result array
    result = {"f0:ec:af:cf:6c:e1":-200,"c9:a6:4d:9b:c0:8c":-200,"c2:b6:6e:70:fa:f7":-200,"d9:5f:f5:4f:10:89":-200,
             "c4:52:32:5c:31:e7":-200,"e9:3c:4a:34:13:fb":-200,"ed:61:e4:e8:22:30":-200,"ea:01:26:75:a4:c3":-200,
              "d0:4e:10:2e:cb:84":-200,"e4:e0:0a:ae:fd:e2":-200,"fa:35:76:56:6f:e3":-200,"d5:b7:dc:69:ca:ae":-200,
             "ca:81:7a:d7:55:49":-200,"e7:2b:ea:2f:95:c5":-200,"d4:32:fc:b5:f0:b5":-200}

    # Define csv file path
    filename = "wifi_data.csv"

    try:
        # Get data as this format : {"id" : "1","UoM_Wireless1" : "-58","eduroam1" : "-89","UoM_Wireless1" : "-89","eduroam1" : "-57"}
        data = ast.literal_eval(data1.decode("utf-8"))
        print(data)

        for key,value in data.items():
            if key in result and result[key] == -200:
                result[key] = float(value)
        
        result["id"] = int(data["id"])

        # Create one row of csv file
        df = pd.Series(result).to_frame().T

        print(df)

        # Write to csv file
        df.to_csv(filename,index=False,mode='a',header=(not os.path.exists(filename)))
        
    except Exception as e:
        print(e)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("mqtt.eclipse.org", 1883, 60)
client.loop_forever()
