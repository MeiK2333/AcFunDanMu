import heapq
import json
import os

import pymongo

client = pymongo.MongoClient(os.environ.get(
    'MONGO_URL', 'mongodb://localhost'))
db = client.acfun


def view_count(limit=20):
    # trans_view_count()
    collections = db.page
    cursor = collections.find().sort('viewCount', -1).limit(limit)
    data = []
    for item in cursor:
        video = {
            'id': item['id'],
            'title': item['title'],
            'viewCount': item['viewCount'],
            'danmuSize': item['danmuSize']
        }
        data.append(video)
    return data


def trans_view_count():
    collections = db.page
    cursor = collections.find()
    for item in cursor:
        if isinstance(item['viewCount'], str) and '万' in item['viewCount']:
            item['viewCount'] = int(float(item['viewCount'][:-1]) * 10000)
            collections.update_one({'_id': item['_id']}, {
                                   '$set': {'viewCount': item['viewCount']}})


def danmu_count(limit=20):
    collections = db.page
    cursor = collections.find().sort('danmuSize', -1).limit(limit)
    data = []
    for item in cursor:
        video = {
            'id': item['id'],
            'title': item['title'],
            'danmuSize': item['danmuSize']
        }
        data.append(video)
    return data


def keyword(limit=20):
    collections = db.danmu
    data = []
    cursor = collections.aggregate([
        {
            '$group': {'_id': '$m', 'count': {'$sum': 1}}
        },
        {
            '$sort': {'count': -1}
        }
    ], allowDiskUse=True)

    cnt = 0
    for item in cursor:
        cnt += 1
        if cnt > limit:
            break
        data.append(item)
    return data


if __name__ == '__main__':
    with open('view_count.json', 'w') as fw:
        fw.write(json.dumps(view_count(), indent=4, ensure_ascii=False))
    # with open('danmu_count.json', 'w') as fw:
    #     fw.write(json.dumps(danmu_count(), indent=4, ensure_ascii=False))
    # with open('keyword.json', 'w') as fw:
    #     fw.write(json.dumps(keyword(), indent=4, ensure_ascii=False))
