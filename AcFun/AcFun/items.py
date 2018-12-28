import scrapy


class AcFunItem(scrapy.Item):
    item_type = scrapy.Field()
    data = scrapy.Field()
