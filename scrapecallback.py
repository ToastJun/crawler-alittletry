import csv
import re
import lxml.html


class ScrapeCallback(object):
    def __init__(self):
        self.writer = csv.writer(open('movies.csv', 'w'))
        self.fields = ('area', 'population', 'iso')
        self.writer.writerow(self.fields)

    def __call__(self, url, html):
        if re.search('places/default/view/', url):
            tree = lxml.html.fromstring(html)
            row = []
            for field in self.fields:
                row.append(tree.cssselect('table > tr#places_{}__row > td.w2p_fw'.format(field))[0].text_content())
            self.writer.writerow(row)
            print("写入url:%s 的数据" % url)
