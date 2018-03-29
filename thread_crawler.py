#!/usr/bin/env python
# coding:utf-8

import sys
import time
from threading import Thread
from urllib import parse
from downloader import Downloader
from mongo_queue import MongoQueue
import multiprocessing

SLEEP_TIME = 1


def thread_crawler(seed_url, user_agent="wswp", headers=None, proxies=None,
                   num_retries=2, cache=None, scrape_callback=None,
                   max_threads_num=5):
    """crawl webpage use multipe threads"""
    crawl_queue = MongoQueue()
    crawl_queue.push(seed_url)

    D = Downloader(1, user_agent, headers, proxies, num_retries, cache)

    def process_task():
        while True:
            try:
                url = crawl_queue.pop()
            except KeyError:
                print("currentlt no urls to process")
                break
            else:
                print("Downloading Thread name is ", sys.thread_info.name)
                html = D(url)
                if scrape_callback:
                    try:
                        links = scrape_callback() or []
                    except Exception as e:
                        print("Error in callback for {}: {}".format(url, e))
                    else:
                        for link in links:
                            link = normalize(seed_url, link)
                            crawl_queue.push(link)
                crawl_queue.complete(url)

    threads = []
    while threads or crawl_queue:
        # the crawl is still alive
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while len(threads) < max_threads_num and crawl_queue:
            thread = Thread(target=process_task)
            thread.setDaemon(True)
            thread.start()
            threads.append(thread)

        time.sleep(SLEEP_TIME)


def normalize(seed_url, link):
    """Normalize this URL by removing hash and adding domain"""
    link, _ = parse.urldefrag(link)
    return parse.join(seed_url, link)


def process_crawler(args, **kwargs):
    num_cpus = multiprocessing.cpu_count()
    print("Starting {} processes".format(num_cpus))
    processes = []
    for i in range(num_cpus):
        p = multiprocessing.Process(target=thread_crawler, args=[args], kwargs=kwargs)
        p.start()
        processes.append(p)

    # wait for processes to complete
    for p in processes:
        p.join()


def main():
    url = "http://www.douban.com"
    thread_crawler(url)


if __name__ == "__main__":
    main()
