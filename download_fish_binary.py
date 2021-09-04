import requests
import json
import sys
import pprint as pp
from pymongo import MongoClient
from bson.binary import Binary
from PIL import Image
import io



json_path = "./metadata/batch01.json"

out = []
for line in open(json_path, 'r'):
    out.append(json.loads(line))



for i in out:
    pp.pprint(i)
    print('\n\n\n')
    
sys.exit()
    
# within each .json, there's three keys
file_name = out[0]['source-ref']
image_annotations = out[0]['batch01']['annotations']
image_details = out[0]['batch01']['image_size'][0]
image_metadata = out[0]['batch01-metadata']

base_url = "https://data.pawsey.org.au/download/FDFML/frames/"
URL = base_url+file_name
r = requests.get(URL, stream=True)

img = Image.open(r.raw)
image_bytes = io.BytesIO()
img.save(image_bytes, format='PNG')

file_size_mb = sys.getsizeof(image_bytes)/1024/1024
image_details.update({'file_size_mb':round(file_size_mb, 2)})

image = {
    'image_id': file_name,
    'image_byte': image_bytes.getvalue(),
    'image_annotations': image_annotations,
    'image_details':  image_details,
    'image_metadata': image_metadata
}

# read it back in
client = MongoClient('mongodb://super_nice_private_ip_address!') # serverSelectionTimeoutMS=2000
db=client.fish
collection=db['fish_bb']

image_id = collection.insert_one(image)



#image = collection.find_one()

#pil_img = Image.open(io.BytesIO(image['data']))
