# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class VNReactionaryNewsItem(scrapy.Item):
    # define the fields for your item here like:
    content = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    created = scrapy.Field()
    author = scrapy.Field()
    # pass