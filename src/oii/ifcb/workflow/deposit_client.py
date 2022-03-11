import sys
import urllib.request, urllib.error, urllib.parse
from urllib.request import urlopen, Request
from urllib.error import HTTPError
import json

class Deposit(object):
    def __init__(self,url_base,product_type='blobs',extension='zip'):
        self.url_base = url_base
        self.product_type = product_type
        self.extension = extension

    def exists(self,pid):
        url = '%s_%s.%s' % (pid, self.product_type, self.extension)
        print('attempting to fetch %s' % url)
        req = urllib.request.Request(url)
        req.get_method = lambda: 'HEAD'
        try:
            resp = urllib.request.urlopen(req)
            return True
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            raise

    def deposit(self,pid,product_file):
        with open(product_file,'r') as inproduct:
            product_data = inproduct.read()
            req = Request('%s/deposit/%s/%s' % (self.url_base, self.product_type, pid), product_data)
            req.add_header('Content-type','application/x-ifcb-blobs')
            resp = json.loads(urlopen(req).read())
            return resp
        
if __name__=='__main__':
    d = Deposit('http://localhost:5063')
    zipfile = 'IFCB8_2010_202_001921_blobs_v2.zip'
    pid = 'http://192.168.1.1/healy/IFCB8_2010_202_001921'
    d.deposit(pid,zipfile)

