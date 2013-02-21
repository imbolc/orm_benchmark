#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import time
import random

import psycopg2


DB_SETTINGS = {
    'dbname': 'orm_benchmark_psycopg2_raw',
}

class Test(object):
    def __init__(self, insert_count, read_count, db_settings=None):
        self.insert_count = insert_count
        self.read_count = read_count
        self.db_settings = db_settings or DB_SETTINGS
        self.ids = range(1, self.insert_count + 1)

    def prepare(self):
        os.system('dropdb %(dbname)s' % self.db_settings)
        os.system('createdb %(dbname)s' % self.db_settings)
        self.connect()
        self.create_table()

    def connect(self):
        conn_string = "dbname=%(dbname)s" % self.db_settings
        self.conn = psycopg2.connect(conn_string)

    def create_table(self):
        cur = self.conn.cursor()
        cur.execute(
            'CREATE TABLE test ('
                ' id     serial PRIMARY KEY,'
                ' num    integer,'
                ' string varchar(10)'
            ');'
        )
        self.conn.commit();

    def test_insert(self):
        started = time.time()
        cur = self.conn.cursor()
        for id in self.ids:
            sql = 'INSERT INTO test (num, string) VALUES (%s, %s);'
            cur.execute(sql, (id, str(id)))
            self.conn.commit()
        ret = time.time() - started
        assert self.get_row_count() == self.insert_count
        return ret

    def test_read_by_pk(self):
        ids = self.ids[:]
        random.shuffle(ids)
        started = time.time()
        cur = self.conn.cursor()
        for id in ids:
            cur.execute("SELECT * FROM test WHERE id=%s;", (id, ))
            cur.fetchone()
        return time.time() - started

    def get_row_count(self):
        cur = self.conn.cursor()
        cur.execute('SELECT count(*) FROM test;')
        return cur.fetchone()[0]

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
