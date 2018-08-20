#-*- coding: utf-8 -*-
import os

from tornado.options import define

dirname = os.path.dirname
base_dir = os.path.abspath(dirname(dirname(__file__)))

define("base_dir", default=base_dir, help="project base dir")
define('port', default = 8888, type = int, help = 'app listen port')
#define('servers', default = ['192.168.1.1'], type = str, help = 'server cluster')

define('mysql_host', default = '100.73.20.3', type = str, help = 'mysql host')
define('mysql_port', default = 13307, type = int, help = 'mysql server listen port')
define('mysql_user', default = 'xxx', type = str, help = 'mysql user')
define('mysql_passwd', default = 'xxx', type = str, help = 'mysql passwd')

define('inception_host', default = '100.73.20.3', type = str, help = 'inception host')
define('inception_port', default = 6669, type = int, help = 'inception server listen port')
define('inception_user', default = '', type = str, help = 'inception user')
define('inception_passwd', default = '', type = str, help = 'inception passwd')