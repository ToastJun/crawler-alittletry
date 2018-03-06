# coding:utf-8


from datetime import timedelta, datetime
from pymongo import MongoClient
import time
import pickle
import zlib
from bson.binary import Binary

from my_crawler import link_crawler


class MongoCache(object):
    """cache html use mongodb"""

    def __init__(self, client=None, expires=timedelta(days=1)):
        # init MongoClient use default port
        client = client if client is not None else MongoClient(
            "localhost", 27017)
        self.db = client.cache

        # create index to expire cached webpages
        self.db.webpage.create_index(
            "timestamp", expireAfterSeconds=expires.total_seconds())

    def __getitem__(self, url):
        """get result by url from cache"""
        result = self.db.webpage.find_one({"_id": url})
        if result:
            print("cache form mongodb _id=%s" % url)
            # decompress html
            html = pickle.loads(zlib.decompress(result['html']))
            return html
        else:
            raise KeyError("no this key", url)

    def __setitem__(self, url, html):
        """save html into mongodb"""
        print("save url=%s \n html:\n%s" % (url, html[:255]))
        record = {"html": Binary(zlib.compress(pickle.dumps(html))), "timestamp": datetime.utcnow()}
        self.db.webpage.update({"_id": url}, {"$set": record}, upsert=True)


if __name__ == "__main__":
    url = "http://www.baidu.com"
    link_crawler(url, "/view", 0, "BaiduSpider", cache=MongoCache())
