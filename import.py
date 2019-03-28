from pymongo import MongoClient, ReturnDocument
from urlparse import urlparse
import pandas as pd
import json
import time 
import sys
import os
from bson.json_util import dumps
root_dir = "./json_files"
path = "json_files/"
domainPath = "./domain/"

def updateDomainDB(db, fname):
    domainColl = db['domainTest']
    domainColl.drop()
    domain = pd.read_json(domainPath + fname)
    print(domain)
    data = domain.to_dict(orient='records')  # Here's our added param..
    domainColl.insert_many(data)


def updateJson(db, usercoll, fname):
    filecoll = db['testFile']
    foundFile = filecoll.find_one({'filename':fname})

    # if (foundFile):
    #     return

    filecoll.insert_one({'filename':fname})
    with open(path + fname,'r') as f:
        for idx, line in enumerate(f):
            if (idx < 10):
                row = json.loads(line)
                found = usercoll.find_one({'author':row['author']})
                if (found):
                    real_count = row['count'] - row['not_identified']
                    old_count = found['count']
                    old_real_count = found['count'] - found['not_identified']
                    new_score = (real_count*row['score'] + old_real_count*found['score']) / (old_real_count + real_count)
                    new_count = old_count + row['count']

                    updateObj = {}
                    set_obj = {
                        'count': new_count,
                        'high': row['high'] + found['high'],
                        'very_high': row['very_high'] + found['very_high'],
                        'low': row['low'] + found['low'],
                        'very Low': row['very Low'] + found['very Low'],
                        'mixed': row['mixed'] + found['mixed'],
                        'not_identified': row['not_identified'] + found['not_identified'],
                        'score': new_score
                    }
                    updateObj['$set'] = set_obj
                    usercoll.find_one_and_update({'_id':found['_id']},updateObj)
                    print("Found")
                    print(found)
                else:
                    authorObj = row
                    usercoll.insert_one(authorObj)
                    print("Insert successful!")
                    print(row['author'])


if __name__ == "__main__":
    jsonFiles = os.listdir(root_dir) 
    print( jsonFiles )
    with MongoClient("mongodb://admin:iimt4601@ds019481.mlab.com:19481/iimt4601") as client:
        db = client.iimt4601
        # updateDomainDB(db, 'output.json')
        for index, fname in enumerate(jsonFiles):
            usercoll = db['testUsers_1']
            updateJson(db, usercoll, fname)