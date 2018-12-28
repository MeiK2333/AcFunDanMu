from AcFun.utils import MongoClient


class AcfunPipeline(object):
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client.acfun

    def process_item(self, item, spider):
        if item.get('item_type') == 'page':
            collections = self.db.page
            collections.update_one({
                'id': item['data']['id']
            }, {'$set': item['data']}, upsert=True)
        elif item.get('item_type') == 'danmu':
            collections = self.db.danmu
            collections.insert_one(item['data'])
        return item

    def close_spider(self, spider):
        self.client.close()
