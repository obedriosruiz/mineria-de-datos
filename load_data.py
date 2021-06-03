#%%
import os

import json
from time import sleep

from google.cloud import pubsub_v1

#%%
base_path = <ROOT FOLDER>

#%%
project_id = <ADD PROJECT ID>
topic_id = <ADD INPUT TOPIC ID>

#%%
file_name = 'datos.json'
path = os.path.join(base_path, 'contents', file_name)
with open(path, 'r') as f:
    info_plant = json.load(f)

#%%
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

cont = 0
for el_data in info_plant:
    data = json.dumps(el_data)
    
    future = publisher.publish(topic_path, data.encode("utf-8"))
    print(future.result())
    
    sleep(1)

print(f"Published messages with error handler to {topic_path}.")