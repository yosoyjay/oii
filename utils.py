# utilities for oii
from threading import local
import os
from hashlib import sha1
from time import time, clock
from unittest import TestCase

genid_prev_id_tl = local()
prev = genid_prev_id_tl.prev = None

def gen_id(namespace=''):
    prev = genid_prev_id_tl.prev
    if prev is None:
        prev = sha1(os.urandom(24)).hexdigest()
    else:
        entropy = str(clock()) + str(time()) + str(os.getpid())
        prev = sha1(prev + entropy).hexdigest()
    genid_prev_id_tl.prev = prev
    return namespace + prev

class test_gen_id(TestCase):
    # run collision test
    def test_all(self):
        for ns in ['', 'http://foo.bar/quux#']:
            ids = []
            len = 5000
            while len > 0:
                len -= 1
                new_id = gen_id(ns)
                assert new_id not in ids
                ids.append(new_id)
        