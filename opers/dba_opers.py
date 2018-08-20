'''
Created on 2017-12-5

@author: zs
'''

import MySQLdb

from tornado.options import options


class DBAOpers(object):

    def get_mysql_connection(self, host ='', user="", passwd='', port=3306, autocommit = True):
        conn = None
        
        try:
            conn=MySQLdb.connect(host, user, passwd, port=port)
            conn.autocommit(autocommit)
            conn.set_character_set('utf8')
        except Exception,e:
            print e
        return conn


    def get_table_data(self, conn, tb_name, db_name):
        
        conn.select_db(db_name)
        cursor = conn.cursor()
        try :
            sql = "select * from `%s`" % (tb_name)
            cursor.execute(sql)
            rows = cursor.fetchall()
        
        except Exception, e :
            print e
        finally:
            cursor.close()
        return rows
    
    
dba_opers = DBAOpers()