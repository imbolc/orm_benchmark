#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
import random

from mongo import Document, Index


class Doc(Document):
    __database__ = 'orm_benchmark_pymongo_mongo'
    __safe__ = True


class Test(object):
    def __init__(self, insert_count, read_count, db_settings=None):
        self.insert_count = insert_count
        self.read_count = read_count

    def prepare(self):
        Doc.remove()

    def test_insert(self):
        started = time.time()
        for i in xrange(self.insert_count):
            Doc(num=i, string=str(i)).save()
        ret = time.time() - started
        assert Doc.count() == self.insert_count
        return ret

    def test_read_by_pk(self):
        ids = [doc.id for doc in Doc.find()]
        random.shuffle(ids)
        started = time.time()
        for id in ids:
            doc = Doc.get_by_id(id)
        ret = time.time() - started
        assert doc.id == id
        return ret


if __name__ == '__main__':
    insert_count, read_count = (sys.argv[1:] if len(sys.argv) == 3
            else (1000, 1000))
    print 'Run test for insert %s rows and read %s rows' % (
            insert_count, read_count)

    test = Test(int(insert_count), int(read_count))
    test.prepare()
    for testname in ['insert', 'read_by_pk']:
        print testname, '...',
        attr = 'test_%s' % testname
        print getattr(test, attr)()
