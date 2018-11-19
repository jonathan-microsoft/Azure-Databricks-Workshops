import json
import requests
from azure.eventhub import EventHubClient, Sender, EventData
from pandas.io.json import json_normalize
import pandas as pd
import time

url = '<market data url>'

usr = '<event uhub user>'

ky = '<event hub key>'

address = 'amqps://<event hub name space>.servicebus.windows.net/quotes'

client = EventHubClient(address, debug=False, username=usr, password=ky)

myResponse = requests.get(url, verify=True)

# For successful API call, response code will be 200 (OK)
if(myResponse.ok):
    
    data = json.loads(myResponse.content.decode("utf-8"))
    data = json_normalize(data)
    data.Price = data.Price.astype('float')
    data.Volume = data.Volume.str.replace(',','').astype('int')
    sender = client.add_sender(partition="0")

    client.run()
    quotes = json.loads(data.to_json(orient='records'))
    for quote in quotes:
        #print(json.dumps(quote))
        sender.send(EventData(json.dumps(quote)))
    client.stop()

#Sleep of minute
time.sleep(60)