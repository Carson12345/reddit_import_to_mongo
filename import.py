from pymongo import MongoClient, ReturnDocument
from urlparse import urlparse
import pandas as pd
import json
import time 
import sys

if __name__ == "__main__":
    with MongoClient("mongodb://admin:iimt4601@ds019481.mlab.com:19481/iimt4601") as client:
        db = client.iimt4601
        usercoll = db['testUsers_1']
        fname = ''
        with open(fname,'r') as f:
            raw = json.loads(f.read())
            print(raw)