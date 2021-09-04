import requests
import json
import sys
import pprint as pp
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
    
    @staticmethod
    def get_n_thread(n,t=list(range(1,20)),get_max=False):
        """
        n: int, number of pages (or ids) to cycle through total from find_last_api_page().
        t: list, range of numbers that reflect the optimal number of threads
        to select from. Should return numbers divisible by n.
        """
        
        l = []
        for i in t:
            if n % i == 0:
                l.append(i)
            
        if not l:
            print('unable to find divisible integer within range, exiting...')
        else:
            if get_max:
                l = max(l)
        return l
    
    # initialise
    def __init__(self, json_path):
        self.json_path = json_path


    def clean_json(self):
        data = []
        output = []
        
        for line in open(self.json_path, 'r'):
            data.append(json.loads(line))

        for d in data:
            # within each .json, there's three keys
            file_name = d['source-ref']
            image_annotations = d['batch01']['annotations']
            image_details = d['batch01']['image_size'][0]
            image_metadata = d['batch01-metadata']

            out_dic = {
                'image_id': file_name,
                'image_annotations': image_annotations,
                'image_details':  image_details,
                'image_metadata': image_metadata
            }

            output.append(out_dic)

        return output
            
    def get_image_binary(self):
        URL = base_url+file_name
        r = requests.get(URL, stream=True)

        img = Image.open(r.raw)
        image_bytes = io.BytesIO()
        img.save(image_bytes, format='PNG')
        image_binary = image_bytes.getvalue()
        
        return image_binary
