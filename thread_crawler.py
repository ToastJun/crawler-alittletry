#!/usr/bin/env python
# coding:utf-8

import sys
import time
from threading import Thread
from urllib import parse
from downloader import Downloader


SLEEP_TIME = 1


def thread_crawler(seed_url, user_agent="wswp", headers=None, proxies=None,
                   num_retries=2, cache=None, scrape_callback=None,
                   max_threads_num=5):
    """crawl webpage use multipe threads"""
    crawl_queue = [seed_url]
    seen = set(crawl_queue)

    D = Downloader(1, user_agent, headers, proxies, num_retries, cache)

    def process_task():
        while crawl_queue:
            try:
                url = crawl_queue.pop()
            except IndexError:
                print("crawl queue raise IndexError")
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
                            if link not in seen:
                                crawl_queue.append(link)
                                seen.add(link)

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


def main():
    url = "http://www.douban.com"
    thread_crawler(url)

    
if __name__ == "__main__":
    main()
