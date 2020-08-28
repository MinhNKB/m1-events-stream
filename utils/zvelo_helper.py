import sys
sys.path.insert(1, '/home/bigdata/luy/python2.6/site-packages/')
from SiteFilterDB import *

class ZveloHelper:
    def __init__(self, path, host, serial):
        # url_debug(1)
        r = url_init(path, 0, host, serial);
        if r:
            print url_errstr(r)

        # update()
        # v4_map_id = url_get_mapping_id("zvelo-v4")
        # rep_map_id = url_get_mapping_id("reputation")
        versions()

    def process_list_urls(self, urls):
        results = []
        for url in urls:
            category0, category1, category2 = self.lookup(url)
            results.append([url, category0, category1, category2])
        return results

    def lookup(self, url):
        category0 = self.catstring(url_lookup(url))
        category1 = self.catmatstring(url_lookup_match(url))
        category2 = self.catmatstring(url_lookup_cache(url))
        # category3 = self.catstring(url_lookup_map(url, rep_map_id))
        # flags = url_flags_s(0)
        # category4 = self.catmatstring(url_lookup_match_map_flags(url, rep_map_id, flags))
        return category0, category1, category2

    def catstring(self, x):
        if len(x) and x[0]:
            if x[0] < 0:
                return url_errstr(x[0])
            else:
                return url_categories_name(x, "| ")
        else:
            return "Uncategorized"

    def catmatstring(self, x):
        return x[-1] + ": " + self.catstring(x[:-1])

if __name__ == '__main__':
    path = ""
    host = ""
    serial = ""
    url = ""
    zvelo_helper = ZveloHelper(path, host, serial)
    print zvelo_helper.lookup(url)