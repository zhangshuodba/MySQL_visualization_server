#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import logging.config

import tornado.ioloop
import tornado.web
import tornado.options

from tornado import httpserver
from appconfig import parse_config
from tornado.options import options
from routes import handlers
from ioloop import IOLoop
from scheduler.schedulerOpers import SchedulerOpers
SchedulerOpers()



class Application(tornado.web.Application):
    def __init__(self):
        
        #settings = dict(
                        #templates_path=os.path.join(os.path.dirname(__file__), 'templates'),
                        #static_path=os.path.join(os.path.dirname(__file__), "static"),
                        #)
        #tornado.web.Application.__init__(self, handlers, **settings)
        tornado.web.Application.__init__(self, handlers)


if __name__ == "__main__":
    http_server = httpserver.HTTPServer(Application())
    
    config_path = os.path.join(options.base_dir, "config")
    logging.config.fileConfig(config_path + '/logging.conf')
    
    tornado.options.parse_command_line()
    http_server.listen(options.port)
    
    IOLoop.instance().start()
    tornado.ioloop.IOLoop.instance().start()
