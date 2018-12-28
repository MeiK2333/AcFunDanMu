import os

import pymongo


def MongoClient():
    client = pymongo.MongoClient(os.environ.get(
        'MONGO_URL', 'mongodb://localhost'))
    return client
