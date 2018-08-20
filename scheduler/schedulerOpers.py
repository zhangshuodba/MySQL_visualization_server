import logging

from tornado.ioloop import PeriodicCallback
from .worker.mysql_worker import MysqlAudit
from opers.dba_opers import dba_opers


class SchedulerOpers(object):

    def __init__(self):

        self.check_mysql_alived_handler(60)

    def check_mysql_alived_handler(self, action_timeout = 55):
        mysql_audit = MysqlAudit(action_timeout)
        period = PeriodicCallback(mysql_audit.mysqlaudit, action_timeout * 1000)
        period.start()
