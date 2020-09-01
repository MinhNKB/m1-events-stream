import sys
from SiteFilterDB import *

class ZveloHelper:
    def __init__(self, path, host = "", serial = ""):
        r = url_init(path, 0, host, serial);
        if r:
            print url_errstr(r)

    def process_list_urls(self, urls):
        results = []
        for url in urls:
            url_ascii = url.decode("ascii", errors="ignore").encode()
            category0, category1, category2 = self.lookup(url_ascii)
            results.append([url, category0, category1, category2])
        return results

    def lookup(self, url):
        categories = self.catstring(url_lookup(url)).split("|")
        while len(categories) < 3:
            categories.append("")
        return categories

    def catstring(self, x):
        if len(x) and x[0]:
            if x[0] < 0:
                return url_errstr(x[0])
            else:
                return url_categories_name(x, "|")
        else:
            return "Uncategorized"