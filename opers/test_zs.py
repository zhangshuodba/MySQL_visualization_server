#!/usr/bin/env python
#coding=utf-8

'''
Created on 2017-12-5

@author: zs
'''
import logging
import traceback

import MySQLdb

from DBUtils.PooledDB import PooledDB

db_pool_ins = None

from tornado.options import options


class DBPool():
    def __init__(self):
        #charset='utf8'
        self.pool = PooledDB(creator=MySQLdb, mincached=1, maxcached=10, maxconnections=100, 
                             blocking=True, host="100.73.20.3", port=13307, user="cathy", passwd="cathy", 
                             charset='utf8',autocommit = True)

    def get_connection(self, *args, **kwargs):
        return self.pool.connection(args)




















class DBAction(object):

    def get_mysql_connection(self, *args, **kwargs):
        global db_pool_ins
        if db_pool_ins == None:
            db_pool_ins = DBPool()
        try:
            self.conn = db_pool_ins.get_connection()
            self.cursor = self.conn.cursor()
	    self.cursor.execute("show binary logs;")
	    print self.cursor.fetchall()
            return self.conn
        except:
            logging.error(traceback.format_exc())
        
dba_opers = DBAction()
dba_opers.get_mysql_connection()
