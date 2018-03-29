#!/usr/bin/env python
# coding=utf-8


from datetime import datetime, timedelta
from pymongo import MongoClient, errors


class MongoQueue(object):
    # possible states of a download
    OUTSTANDING, PROCESSING, COMPLETE = range(3)

    def __init__(self, client=None, timeout=30):
        self.client = client or MongoClient()
        self.db = self.client.cache
        self.timeout = timeout

    def __nonzero__(self):
        """Return True if there are more jobs to process"""
        record = self.db.crawl_queue.find_one({'state': {'$ne': self.COMPLETE}})
        return True if record else False

    def push(self, url):
        """Add new URL to queue if does not exist"""
        try:
            self.db.crawl_queue.insert({"_id": url, "state": self.OUTSTANDING})
        except errors.DuplicateKeyError as e:
            pass  # this is already in the queue

    def pop(self):
        """Get an outstanding URL from the queue and set its state to processing. If the queue is empty a KeyError exception is raised"""
        record = self.db.crawl_queue.find_and_modify(
            query={"state": self.OUTSTANDING},
            update={"$set": {"state": self.PROCESSING,
                             "timestamp": datetime.now()}}
        )
        if record:
            return record['_id']
        else:
            self.repair()
            raise KeyError()

    def complete(self, url):
        """Update this URL state to COMPLETE"""
        self.db.crawl_queue.update({"_id": url},{"$set": {"state": self.COMPLETE}})

    def repair(self):
        """Release stalled jobs"""
        record = self.db.crawl_queue.find_and_modify(
            query={
                "timestamp": {"$le": datetime.now()-timedelta(second=self.timeout)},
                "state": {"$ne": self.COMPLETE}
            },
            update={'$set': {'state': self.OUTSTANDING}}
        )
        if record:
            print 'Released:', record['_id']
