import requests
import json
import sys
import pprint as pp
from pathlib import Path
from tqdm import tqdm
from pymongo import MongoClient
from bson.binary import Binary
from PIL import Image
import io


class ripFiles:
    """"
    This class will grab the filenames as provided by batch.json files found
    here: https://data.pawsey.org.au/public/?path=/FDFML/labelled/manifests
    then, you can download and send them to Mongo!
    """

    # pretty much everything goes through here
    base_url = "https://data.pawsey.org.au/download/FDFML/frames/"
        
    # initialise
    def __init__(self, json_path, collection):
        self.json_path = json_path
        self.collection = collection


    def get_json(self):
        data=[]
        for line in open(self.json_path, 'r'):
            data.append(json.loads(line))
            
        return data

    def get_file_names(self):
        files = []
        data = self.get_json()

        for d in data:
            # within each .json, there's three keys
            file_name = d['source-ref']
            files.append(file_name)

        return files

    def clean_json(self):
        output = []
        batch = Path(self.json_path).stem

        data = self.get_json()

        for d in data:
            # within each .json, there's three keys
            file_name = d['source-ref']
            image_annotations = d[batch]['annotations']
            image_details = d[batch]['image_size'][0]
            image_metadata = d[batch + '-metadata']

            out_dic = {
                'image_id': file_name,
                'image_annotations': image_annotations,
                'image_details':  image_details,
                'image_metadata': image_metadata
            }

            output.append(out_dic)

        return output
            
    def get_image_binary(self, file_names):

        for f in tqdm(file_names):
            # requests
            URL = self.base_url+f['image_id']
            r = requests.get(URL, stream=True)
            # image processing
            img = Image.open(r.raw)
            image_bytes = io.BytesIO()
            img.save(image_bytes, format='PNG')
            image_binary = image_bytes.getvalue()
            # send it to mongo
            f.update({'image_byte': image_binary})
            image_id = self.collection.insert_one(f).inserted_id

        return 

        



