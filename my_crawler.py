#!/usr/bin/env python
# coding:utf-8

from urllib import parse, error, request, robotparser
import re

import scrapecallback
from downloader import Downloader, Thottle


def link_crawler(seed_url, webpage_regex="", max_depth=2, user_agent="wswp", num_retries=2, proxies=None, headers=None, scrape_callback=None, cache=None):
    """链接爬虫"""
    crawl_queue = [seed_url]
    # 已经爬取过的网页及对应深度字典
    seen = {seed_url: 0}
    rp = get_robots(seed_url)

    downloader = Downloader(3, user_agent=user_agent, headers=headers, proxies=proxies, num_retries=num_retries, cache=cache)

    while crawl_queue:
        url = crawl_queue.pop()
        # check url passes robots.txt restrictions
        if rp.can_fetch(user_agent, url):
            html = downloader(url)

            depth = seen.get(url)
            if depth == max_depth:
                continue

            links = []
            if scrape_callback:
                links.extend(scrape_callback(url, html) or [])

            # 获取html中的链接 以便放入任务队列继续爬取
            links.extend(link for link in get_links(html) if regex_link(link)) # 符合规范的url才能加入队列

            for link in links:
                # 拼接正确的url
                link = parse.urljoin(seed_url, link)
                if link not in seen:
                    crawl_queue.append(link)
                    seen[link] = depth + 1
        else:
            print("Blocked by robots.txt:", url)


def get_links(html):
    webpage_regex = re.compile(r'<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
    links = None
    if html:
        links = webpage_regex.findall(html)
    return links or []


def regex_link(link):
    return link


def get_robots(url):
    """Initialize robots parser for this domain"""
    rp = robotparser.RobotFileParser()
    rp.set_url(parse.urljoin(url, '/robots.txt'))
    rp.read()
    return rp


if __name__ == "__main__":
    url = "http://example.webscraping.com"
    link_crawler(url, webpage_regex=".*?/(view|index)", scrape_callback=scrapecallback.ScrapeCallback())
