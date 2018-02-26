#!/usr/bin/env python
# coding:utf=8

from urllib import request, error, parse, robotparser
import random
import datetime
import time


DEFAULT_USERAGENT = "wswp"


class Downloader(object):
    """网页下载类"""
    def __init__(self, delay=5, user_agent=DEFAULT_USERAGENT, headers=None, proxies=None, num_retries=2, cache=None):
        self.headers = headers or {}
        if user_agent:
            self.headers['User-agent'] = user_agent
        self.num_retries = num_retries
        self.proxies = proxies
        self.thottle = Thottle(delay)
        self.cache = cache

    def __call__(self, url):
        result = None
        if self.cache:
            try:
                result = self.cache[url]
                print("read from cache")
            except KeyError:
                pass
            else:
                if result and result['code']:
                    if self.num_retries > 0 and \
                        500 <= result['code'] < 600:
                        result = None

        if result is None:
            # result was not loaded from cache
            # so still need to download
            self.thottle.wait(url)
            proxy = random.choice(self.proxies) if self.proxies else None
            result = self.download(url, proxy, self.num_retries)
            if self.cache:
                self.cache[url] = result

        return result["html"]

    def download(self, url, proxy=None, num_retries=2):
        req = request.Request(url, headers=self.headers)

        opener = request.build_opener()
        if proxy:
            proxy_params = {parse.urlparse(url).schme: proxy}
            opener.add_handler(request.ProxyHandler(proxy_params))

        try:
            self.thottle.wait(url)
            response = opener.open(req)
            html = response.read().decode()
            code = response.code
        except Exception as e:
            print("url: %s \nDownload Error: %s" % (url, str(e)))
            html = None
            if hasattr(e, "code"):
                code = e.code
                if num_retries > 0 and 500 <= e.code < 600:
                    html = self.download(url, num_retries-1)
            else:
                code = None
        return {"html": html, "code": code}


class Thottle(object):
    "Add a delay between downloads to the same domain"
    def __init__(self, delay=0):
        self.delay = delay
        self.domains = {}

    def wait(self, url):
        # 获取url的domain
        domain = parse.urlparse(url).netloc
        last_accessed = self.domains.get(domain)

        if self.delay > 0 and last_accessed is not None:
            # 判断是否需要延迟爬取
            sleep_secs = self.delay - (last_accessed - datetime.datetime.now()).seconds
            if sleep_secs > 0:
                print("sleep %d" % sleep_secs)
                time.sleep(sleep_secs)

        # 将爬取过的domain访问时间进行更新
        self.domains[domain] = datetime.datetime.now()
