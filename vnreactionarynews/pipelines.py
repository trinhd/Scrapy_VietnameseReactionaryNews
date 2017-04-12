# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

#import json
#import codecs
import pymongo

from pymongo import MongoClient

class VNReactionaryNewsPipeline(object):
    def __init__(self, client):
        self.client = client

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            client = MongoClient()
        )

    def process_item(self, item, spider):
        
        """client = MongoClient()
        db = client.allvnexpress
        collAll = db.all
        oneRow = {
            "subject": item["subject"],
            "link": item["link"],
            "title": item["title"],
            "content": item["content"]
        }
        collAll.insert_one(oneRow)
        collAll.insert(dict(item))"""
        self.db.all.insert(dict(item))

        return item
        
    def open_spider(self, spider):
        self.db = self.client.dantriblog

    def close_spider(self, spider):
        self.client.close()
