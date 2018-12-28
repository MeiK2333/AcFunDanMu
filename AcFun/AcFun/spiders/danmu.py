# -*- coding: utf-8 -*-
import json
import os
import re

import scrapy
from scrapy_redis.spiders import RedisCrawlSpider

from AcFun.items import AcFunItem


class DanmuSpider(RedisCrawlSpider):
    name = 'danmu'
    allowed_domains = ['www.acfun.cn', 'danmu.aixifan.com']
    redis_key = 'danmu:start_urls'
    # 主动处理 302，否则错误日志会刷屏
    handle_httpstatus_list = [302]

    def start_requests(self):
        ac_start = os.environ.get('ac_start', '4166948')
        ac_end = os.environ.get('ac_end', '4819800')  # 截至我写代码时的最后一个
        for i in range(int(ac_start), int(ac_end) + 1):
            yield scrapy.Request(f'http://www.acfun.cn/v/ac{i}', callback=self.parse_page)

    def parse_page(self, response):
        # 重定向不做处理
        if response.status in (301, 302):
            return
        js_data = re.search(
            r'var pageInfo = {.*?}<', response.body_as_unicode())
        data = json.loads(js_data.group()[15:-1])
        item = AcFunItem()
        item['item_type'] = 'page'
        item['data'] = data
        yield item

        for video in data['videoList']:
            # 仅获取 2 类型的弹幕（高级弹幕也很难分析）
            yield scrapy.Request(f'http://danmu.aixifan.com/V4/{video["source_id"]}_2/0/1000?order=1', meta={'vid': video['source_id']})

    def parse(self, response):
        data = json.loads(response.body_as_unicode())[2]
        max_time = 0
        vid = response.meta['vid']

        for i in data:
            i['vid'] = response.meta['vid']
            i['c'] = i['c'].split(',')
            i['c'][5] = int(i['c'][5])
            max_time = max(max_time, i['c'][5])

            item = AcFunItem()
            item['item_type'] = 'danmu'
            item['data'] = i
            yield item

        # 如果本页有 1000 条数据，则尝试获取下一页
        if len(data) == 1000:
            yield scrapy.Request(f'http://danmu.aixifan.com/V4/{vid}_2/{max_time}/1000?order=1', meta={'vid': vid})
