#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import time
import random

import psycopg2
from peewee import PostgresqlDatabase, Model, CharField, IntegerField


DBNAME = 'orm_benchmark_psycopg2_peewee'

class TestModel(Model):
    num = IntegerField()
    string = CharField(max_length=10)

    class Meta:
        database = PostgresqlDatabase(DBNAME)


class Test(object):
    def __init__(self, insert_count, read_count):
        self.insert_count = insert_count
        self.read_count = read_count
        self.ids = range(1, self.insert_count + 1)

    def prepare(self):
        os.system('dropdb %s' % DBNAME)
        os.system('createdb %s' % DBNAME)
        self.connect()
        self.create_table()

    def connect(self):
        conn_string = "dbname=%s" % DBNAME
        self.conn = psycopg2.connect(conn_string)

    def create_table(self):
        cur = self.conn.cursor()
        cur.execute(
            'CREATE TABLE testmodel ('
                ' id     serial PRIMARY KEY,'
                ' num    integer,'
                ' string varchar(10)'
            ');'
        )
        self.conn.commit();

    def test_insert(self):
        started = time.time()
        for id in self.ids:
            TestModel(num=id, string=str(id)).save()
        ret = time.time() - started
        TestModel.select().count() == self.insert_count
        return ret

    def test_read_by_pk(self):
        ids = self.ids[:]
        random.shuffle(ids)
        started = time.time()
        for id in ids:
            obj = TestModel.get(TestModel.id == id)
        ret = time.time() - started
        assert obj.string == str(id)
        return ret


if __name__ == '__main__':
    insert_count, read_count = (sys.argv[1:] if len(sys.argv) == 3
            else (1000, 1000))
    print 'Run test for insert %s rows and read %s rows' % (
            insert_count, read_count)

    test = Test(int(insert_count), int(read_count))
    test.prepare()
    for testname in [
            'insert',
            'read_by_pk'
            ]:
        print testname, '...',
        attr = 'test_%s' % testname
        print getattr(test, attr)()
