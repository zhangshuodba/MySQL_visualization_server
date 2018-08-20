#coding:utf-8

'''
Created on Nov 30, 2017

'''
import traceback
import logging
import subprocess
import os
import re

from concurrent.futures import ThreadPoolExecutor
from tornado.web import asynchronous
from tornado.gen import coroutine
from tornado.concurrent import run_on_executor
from tornado.escape import json_decode, json_encode
from ast import literal_eval
import jwt
import datetime
#from datetime import datetime, timedelta

from handlers.base import BaseHandler
from opers.dba_opers import dba_opers
from tornado.options import options
import json
import simplejson,ast
import MySQLdb


"""
    JSON Web Token auth for Tornado
"""

AUTHORIZATION_HEADER = 'Authorization'
AUTHORIZATION_METHOD = 'bearer'
SECRET_KEY = "my_secret_key"
INVALID_HEADER_MESSAGE = "invalid header authorization"
MISSING_AUTHORIZATION_KEY = "Missing authorization"
AUTHORIZATION_ERROR_CODE = "401"
AUTHORIZATION_HAS_EXPIRED = "403"

jwt_options = {
        'verify_signature': True,
        'verify_exp': True,
        'verify_nbf': False,
        'verify_iat': True,
        'verify_aud': False
}


def create_process(cmd):
	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	result = p.stdout.read()
	code = p.wait()
	return code, result


class DateEncoder(json.JSONEncoder):  
	def default(self, obj):  
		if isinstance(obj, datetime.datetime):  
			return obj.strftime('%Y-%m-%d %H:%M:%S')  
		elif isinstance(obj, date):  
			return obj.strftime("%Y-%m-%d")
		elif isinstance(obj, decimal.Decimal):
			return str(obj)
		else:  
			return json.JSONEncoder.default(self, obj) 


class AdminServerHandler(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')    
		args = self.get_all_arguments()
		ret = yield self.do(args)
		self.finish({'message': ret})

	@run_on_executor
	def do(self, args):
		try:
			ip = args.get('ip')
			port = args.get('port')
			db = args.get('db')
			user = args.get('user')
			password = args.get('password')
			sql_text = args.get('sql_text')
			conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
			conn.select_db('release_system')
			cursor = conn.cursor()
			sql = "INSERT INTO execute_sql_info(ip, port, db, user, password, sql_text) VALUES('%s', '%s', '%s', '%s', '%s', '%s');" % (ip, port, db, user, password, sql_text)
			cursor.execute(sql)

		except:
			print traceback.format_exc()
		finally:
			cursor.close()
			conn.close()
		return 'success'



class AddDBInfo(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		try:
			
		
			self.set_header("Access-Control-Allow-Origin", "*")
			self.set_header("Access-Control-Allow-Headers", "Authorizssation")
			self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
			args = self.get_textplain_argument()
			#args = self.get_all_arguments()
			ret = yield self.do(args)
			self.finish({'message': ret})
		except:
			print traceback.format_exc()
			
				


	def options(self, *args, **kwargs): 
		try:


			self.set_header("Access-Control-Allow-Origin", "*")
			self.set_header("Access-Control-Allow-Headers", "Authorizssation")
			self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
		except:
			print traceback.format_exc()


	@run_on_executor
	def do(self, args):
		try:
			ins_name = args.get('instance_name')
			ip = args.get('ip')
			#vip = args.get('vip')
			port = args.get('port')
			dbname = args.get('dbname')
			conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
			conn.select_db('release_system')
			cursor = conn.cursor()
			sql = "INSERT INTO instance(ins_name, ip, port, dbname) VALUES('%s', '%s', '%s', '%s');" % (ins_name, ip, port,dbname)
			cursor.execute(sql)

		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
			conn.close()
		return 'success'
	
	
class processList(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_textplain_argument()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		_list = []
		user = args.get('user')
		password = args.get('password')
		host = args.get('ip')
		port = int(args.get('port'))
					
		try:
			conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,charset="utf8")
			cursor = conn.cursor()
			sql = "show full processlist"
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows:
				_db = str(row[3])
				_Info = str(row[7])
				
				if _db == "None":
					db = ""
				else:
					db = row[3]
					
				if _Info == "None":
					Info = ""
				else:
					Info = row[7]
					
				Id = row[0]
				User = row[1]
				Host = row[2]
				#db = row[3]
				Command = row[4]
				Time = row[5]
				State = row[6]
				#Info = row[7]
				_list.append({"Id": Id,"User": User,"Host": Host,"db":db,"Command":Command,"Time":Time,"State":State,"Info":Info})
			data = json.dumps(_list)

		except:
			print traceback.format_exc()
		finally:
			cursor.close()
			conn.close()
		return data

class dbInfo(BaseHandler): 

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def get(self):
		ret = yield self.do()
		self.finish(ret)		

	@run_on_executor
	def do(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "Authorizssation")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS') 		
		try:
			_list = []
			conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
			conn.select_db('release_system')
			cursor = conn.cursor()
			sql = "select `ins_name`, `ip`, `port`, `dbname` from release_system.instance"
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows:
				instance_name = row[0]
				ip = row[1]
				#vip = row[2]
				port = row[2]
				dbname = row[3]
				_list.append({"instance_name": instance_name,"ip": ip,"port":port,"dbname":dbname})
			data = json.dumps(_list)
		except:
			print traceback.format_exc()
		finally:
			cursor.close()
			conn.close()
		return data


class historySQL(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def get(self):
		ret = yield self.do()
		self.finish(ret)		

	@run_on_executor
	def do(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "Authorizssation")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS') 		
		try:
			_list = []
			conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
			conn.select_db('release_system')
			cursor = conn.cursor()
			sql = "select `ip`, `dbname`, `submint_time`, `status`, `id` from release_system.history_execute_sql order by id desc"
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows:
				ip = row[0]
				dbname = row[1]
				submint_time = row[2].strftime('%c')
				status = row[3]
				id = row[4]
				_list.append({"ip": ip,"dbname": dbname,"submint_time": submint_time,"status":status,"id":id})
			data = json.dumps(_list)
		except:
			print traceback.format_exc()
		finally:
			cursor.close()
			conn.close()
		return data

class ShowServerListHandler(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def get(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "Authorizssation")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')    
		#print (self.request.headers)
		auth = self.request.headers["Authorizssation"]
		if auth:
			parts = auth.split()
			
			if parts[0] != AUTHORIZATION_METHOD:
				self.finish(AUTHORIZATION_ERROR_CODE)
				
			token = parts[1]
			try:
				jwt.decode(
				        token,
				        SECRET_KEY,
				        options=jwt_options
				        )
				ret = yield self.do()
				self.finish(ret)
				
				
			except Exception as err:
				err = str(err)
				if str(err) == "Signature has expired":
					self.finish(AUTHORIZATION_HAS_EXPIRED)
				else:
					self.finish(AUTHORIZATION_ERROR_CODE)
		else:
			self.finish(AUTHORIZATION_HAS_EXPIRED)

	def options(self, *args, **kwargs): 
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "Authorizssation")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            


	@run_on_executor
	def do(self):

		try:
			_list = []
			conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
			conn.select_db('release_system')
			cursor = conn.cursor()
			sql = "select ip,port,db from execute_sql_info;"
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows:
				ip = row[0]
				port = row[1]
				db = row[2]
				_list.append({"ip": ip,"port": port,"db": db})
			data = json.dumps(_list)

		except:
			print traceback.format_exc()
		finally:
			conn.close()
		return data


class InceptionHandler(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_textplain_argument()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		try:
			_list = []
			_json = []
			user = args.get('user')
			password = args.get('password')
			host = args.get('ip')
			operate = args.get('operate')
			port = args.get('port')
			db = args.get('dbname')
			sql_text = args.get('sql_text')
			
			if operate =="execute":
				try:
					conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
					conn.select_db('release_system')
					cursor = conn.cursor()
					sql = "INSERT INTO release_system.history_execute_sql(ip, dbname, status) VALUES('%s', '%s','running');" % (host, db)
					cursor.execute(sql)
					cursor.execute("select last_insert_id();")
					last_insert_id = cursor.fetchall()
					last_insert_id = last_insert_id[0][0]
					cursor.close()
				except:
					logging.error(traceback.format_exc())
				
				try:
					conn = dba_opers.get_mysql_connection(options.inception_host, options.inception_user, options.inception_passwd, options.inception_port, True)
					cursor = conn.cursor()
					sql='''/*--user=%s;--password=%s;--host=%s;--enable-%s;--port=%s;*/
				
							    inception_magic_start;
							    set names utf8;
							    use %s;
							    %s
							    inception_magic_commit;'''            
					#cursor.execute('SET NAMES utf8;')
					cursor.execute(sql % (user,password,host,operate,port,db,sql_text))
					#num_fields = len(cursor.description)
					#field_names = [i[0] for i in cursor.description]
					result = cursor.fetchall()
					cursor.close()
					data = json.dumps(result)
				
					try:
						for row in result:
							if row[2] != 0:
								ince_sub_status = "error"
								break
							else:
								ince_sub_status = "ok"
						conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
						conn.select_db('release_system')
						cursor = conn.cursor()
						sql = "update release_system.history_execute_sql set status = '%s' where id = %s" %(ince_sub_status,last_insert_id)
						cursor.execute(sql)					
						try:
							for row in result:
								_list.append('{"ID":"%s", "stage":"%s", "errlevel":"%s", "stagestatus":"%s", "errormessage":"%s", "SQL":"%s", "Affected_rows":"%s","sequence":"%s", "backup_dbname":"%s", "execute_time":"%s"}' %(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))
				
							#for i in _list:
								#_json.append(json.loads(i))
				
							#_json = ast.literal_eval(json.dumps(_json))
							#_json = simplejson.dumps(_json)
							_json = json.dumps(_list)
							sql = "update release_system.history_execute_sql set description = '%s' where id = %s" %(MySQLdb.escape_string(_json),last_insert_id)
							cursor.execute(sql)
							cursor.close()
						except:
							logging.error(traceback.format_exc())
				
					except:
						logging.error(traceback.format_exc())
				
				except:
					logging.error(traceback.format_exc())			
				
			elif operate =="check":
				
				try:
					conn = dba_opers.get_mysql_connection(options.inception_host, options.inception_user, options.inception_passwd, options.inception_port, True)
					cursor = conn.cursor()
					sql='''/*--user=%s;--password=%s;--host=%s;--enable-%s;--port=%s;*/
				
							    inception_magic_start;
							    set names utf8;
							    use %s;
							    %s
							    inception_magic_commit;'''            
					#cursor.execute('SET NAMES utf8;')
					cursor.execute(sql % (user,password,host,operate,port,db,sql_text))
					result = cursor.fetchall()
					cursor.close()
					data = json.dumps(result)
				except:
					logging.error(traceback.format_exc())
		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
		return data

class SubmitedSQLDetail(BaseHandler):
	
	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		try:
			self.set_header("Access-Control-Allow-Origin", "*")
			self.set_header("Access-Control-Allow-Headers", "Authorizssation")
			self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
			args = self.get_textplain_argument()
			ret = yield self.do(args)
			self.finish(ret)
		except:
			logging.error(traceback.format_exc())
			
				
	def options(self, *args, **kwargs): 
		try:


			self.set_header("Access-Control-Allow-Origin", "*")
			self.set_header("Access-Control-Allow-Headers", "Authorizssation")
			self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
		except:
			logging.error(traceback.format_exc())
	
	
	
	
	@run_on_executor
	def do(self, args):		
		try:
			id = args.get('id')
			conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
			cursor = conn.cursor()
			sql = "select `description` from release_system.history_execute_sql where id = %s" %id
			cursor.execute(sql)
			rows = cursor.fetchall()
			rows = rows[0][0]
			_list = literal_eval(rows)
			
			data = json.dumps(_list)
		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
			conn.close()
		return data

class Kill(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_textplain_argument()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		
		_list = []
		_id = []
		 
		user = args[-1].get('user')
		password = args[-1].get('password')
		host = args[-1].get('ip')
		port = int(args[-1].get('port'))
		for i in args[:-1]:
			_id.append(i.get('Id'))
			#if i.get('User') == "xxx":
				#break
			#else:
				#_id.append(i.get('Id'))
					
		try:
			conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,charset="utf8")
			cursor = conn.cursor()

			for i in _id:
				sql = "kill %s;" %int(i)
				cursor.execute(sql)

			rows = cursor.fetchall()
			
			#for row in rows:
				#Id = row[0]
				#User = row[1]
				#Host = row[2]
				#db = row[3]
				#Command = row[4]
				#Time = row[5]
				#State = row[6]
				#Info = row[7]
				#_list.append({"Id": Id,"User": User,"Host": Host,"db":db,"Command":Command,"Time":Time,"State":State,"Info":Info})
			data = json.dumps(rows)

		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
			conn.close()
		return data


class Login(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')    
		args = self.get_all_arguments()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		try:
			username = args.get('username')
			password = args.get('password')
			
			_list = []
			conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
			conn.select_db('release_system')
			cursor = conn.cursor()
			sql = "select user_name,user_password,user_id from jwt_auth;"
			cursor.execute(sql)
			rows = cursor.fetchall()
			for row in rows:
				if username == row[0] and password == row[1]:
					user_name = row[0]
					user_password = row[1]
					user_id = row[2]
					_list.append({"user_name": user_name,"user_password": user_password,"user_id": user_id}) 
				else:
					continue
				
				
			try:
				encoded = jwt.encode({
			                'user_name': _list[0].get('user_name'),
			                'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=600)},
			                             SECRET_KEY,
			                             algorithm='HS256'
			                             )
				response = {'token':encoded.decode('ascii')}				
				

			except:
				response = {'message':AUTHORIZATION_ERROR_CODE}

		except:
			logging.error(traceback.format_exc())
		return response



class SlowLog(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_all_arguments()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		user = args.get('user')
		password = args.get('password')
		host = args.get('ip')
		port = int(args.get('port'))	
		
		os_password = args.get('os_password')
		os_user = args.get('os_user')
		try:
			conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,charset="utf8")
			cursor = conn.cursor()
			cursor.execute("select concat(@@datadir,@@slow_query_log_file);")
			rows = cursor.fetchall()
			path = rows[0][0]
			
			
			try:
				slow_file = '/data/slow/%s' %('_'.join(host.split('.'))+'_'+str(port)+'_'+'slow')
				slowlogcp = "sshpass -p %s scp -o StrictHostKeyChecking=no %s@%s:%s %s" %(os_password,os_user,host,path,slow_file)
				create_process(slowlogcp)
				cmd = "pt-query-digest --user=xxx --password=xxx --review h='100.73.20.3',P=13307,D=slow,t=%s --no-report --create-review-table   %s" %('_'.join(host.split('.'))+'_'+str(port),slow_file)
				create_process(cmd)
				os.remove(slow_file)
			except:
				logging.error(traceback.format_exc())

			

		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
			conn.close()
		return "ok"


class BinLog(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_all_arguments()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		
		user = args.get('user')
		password = args.get('password')
		host = args.get('ip')
		port = int(args.get('port'))						
		try:
			conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,charset="utf8")
			cursor = conn.cursor()
			cursor.execute("show binary logs;")
			rows = cursor.fetchall()
			data = json.dumps(rows)
			
		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
			conn.close()
		return data
	
	
class DDL(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		#args = self.get_all_arguments()
		args = self.get_textplain_argument()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		host = args.get('ip')
		port = args.get('port')
		db = args.get('dbname')
		sql_text = args.get('sql_text')
		sql_text = sql_text.replace('\"', '\'')
		sql_text = sql_text.replace('`', '\`')
		operate = args.get('operate')
		try:
			if operate == 1:
				try:
					cmd = 'ddl_manager -h %s -d %s  -P %s -a "%s" -j %s' %(host,db,port,sql_text,operate)
					logging.info('sql_check:'+'%s' % cmd)
					res = create_process(cmd)
					data = list(res)
					log_path = res[1]
					log_path = ''.join(log_path.split('\n'))
					_log = open(log_path, "r")
					_list = []
					for line in _log.readlines():
						line = line.strip()
						_list.append(line)
					_log.close()
					data.append(_list)
					if data[0] ==0:
						data = {"code":data[0],"log_path":data[1],"log_message":' '.join(data[2][-1].split(' ')[4:])}
					else:
						data = {"code":data[0],"log_path":data[1],"log_message":' '.join(data[2][-1].split(' ')[4:])}
						
					data = json.dumps(data)
					
				except:
					logging.error(traceback.format_exc())
				
			elif operate == 0:
				try:
					conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
					conn.select_db('release_system')
					cursor = conn.cursor()
					sql = "INSERT INTO release_system.history_execute_sql(ip, dbname, status) VALUES('%s', '%s','running');" % (host, db)
					cursor.execute(sql)
					cursor.execute("select last_insert_id();")
					last_insert_id = cursor.fetchall()
					last_insert_id = last_insert_id[0][0]
					cursor.close()
				except:
					logging.error(traceback.format_exc())
					
				
				try:
					cmd = 'ddl_manager -h %s -d %s  -P %s -a "%s" -j %s' %(host,db,port,sql_text,operate)
					logging.info('sql_execute:'+'%s' % cmd)
					res = create_process(cmd)
					data = list(res)
					log_path = res[1]
					log_path = ''.join(log_path.split('\n'))
					_log = open(log_path, "r")
					_list = []
					for line in _log.readlines():
						line = line.strip()
						_list.append(line)
					_log.close()
					_json = json.dumps(_list)
					
					if data[0] ==0:
						ddl_sub_status = "ok"
					else:
						ddl_sub_status = "error"
						
					conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
					conn.select_db('release_system')
					cursor = conn.cursor()
					sql = "update release_system.history_execute_sql set status = '%s' where id = %s" %(ddl_sub_status,last_insert_id)
					cursor.execute(sql)
					sql = "update release_system.history_execute_sql set description = '%s' where id = %s" %(MySQLdb.escape_string(_json),last_insert_id)
					cursor.execute(sql)
					cursor.close()
					
					data.append(_list)
					if data[0] ==0:
						data = {"code":data[0],"log_path":data[1],"log_message":' '.join(data[2][-1].split(' ')[4:])}
					else:
						data = {"code":data[0],"log_path":data[1],"log_message":' '.join(data[2][-1].split(' ')[4:])}
					data = json.dumps(data)
					
				except:
					logging.error(traceback.format_exc())			
		except:
			logging.error(traceback.format_exc())
		return data
	

class SQLEXCUTE(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_all_arguments()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		
		user = args.get('user')
		password = args.get('password')
		host = args.get('ip')
		port = int(args.get('port'))
		sql = args.get('sql')
		#db = args.get('db')
					
		try:
			conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,charset="utf8")
			#conn.select_db(db)
			cursor = conn.cursor()
			cursor.execute(sql)
			rows = cursor.fetchall()
			data = json.dumps(rows, cls=DateEncoder)

		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
			conn.close()
		return data


class MySQLAudit(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_all_arguments()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		
		user = args.get('user')
		password = args.get('password')
		host = args.get('ip')
		port = int(args.get('port'))			
		try:
			status_name = []
			status_value = []
			variables_name = []
			variables_value = []
			sql_user_list = []
			sql_status = "show global status"
			sql_variables = "show global variables"
			sql_user = "SELECT CONCAT(user, '\@', host) info FROM mysql.user WHERE authentication_string = '' OR authentication_string IS NULL"
			table_audit = "SELECT ENGINE,SUM(DATA_LENGTH)+ SUM(index_length),COUNT(ENGINE) FROM information_schema.TABLES WHERE TABLE_SCHEMA NOT IN ('information_schema','mysql', 'performance_schema','sys') AND ENGINE IS NOT NULL GROUP BY ENGINE ORDER BY ENGINE ASC"
			conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,charset="utf8")
			cursor = conn.cursor()
			cursor.execute(sql_status)
			sql_status = cursor.fetchall()
			for row in sql_status:
				status_name.append(row[0])
				status_value.append(row[1])
			status_dict = dict(zip(status_name, status_value))
			cursor.execute(sql_variables)
			sql_variables = cursor.fetchall()
			for row in sql_variables:
				variables_name.append(row[0])
				variables_value.append(row[1])
			variables_dict = dict(zip(variables_name, variables_value))
			cursor.execute(sql_user)
			sql_user = cursor.fetchall()
			for row in sql_user:
				sql_user_list.append(row)
			cursor.execute(table_audit)
			table_audit = cursor.fetchall()
			
			for row in table_audit:
				table_audit_dict = {"engine":row[0],"data_size":str(row[1]),"innodb_table_nu":str(row[2])}
			total_writes = int(status_dict.get('Com_delete')) + int(status_dict.get('Com_insert')) + int(status_dict.get('Com_update')) + int(status_dict.get('Com_replace'))
			total_reads = int(status_dict.get('Com_select'))
			total_qps = int(total_writes + total_reads)
			audit_list = {
			        "Uptime":status_dict.get('Uptime'),
			        "QPS":int(status_dict.get('Com_select')) + int(status_dict.get('Com_insert')) + int(status_dict.get('Com_update')) + int(status_dict.get('Com_delete')) + int(status_dict.get('Com_replace')),
			        "TPS":int(status_dict.get('Com_insert')) + int(status_dict.get('Com_update')) + int(status_dict.get('Com_delete')),
			        "Threads_running":status_dict.get('Threads_running'),
			        "pct_slow_queries":"%.2f%%" % (float(status_dict.get('Slow_queries')) / float(status_dict.get('Questions')) * 100),
			        "pct_reads":"%.2f%%" % (float(total_reads) / float(total_qps) * 100),  
			        "pct_writes":"%.2f%%" % ((float(1) - float(total_reads) / float(total_qps)) * 100),
			        "pct_connections_used":"%.2f%%" % (float(status_dict.get('Max_used_connections')) / float(variables_dict.get('max_connections')) * 100),
			        "pct_aborted_connections":"%.2f%%" % (float(status_dict.get('Aborted_connects')) / float(status_dict.get('Connections')) * 100),
			        "Bytes_received":str(int(status_dict.get('Bytes_received'))/1024/1024) + 'M',
			        "Bytes_sent":str(int(status_dict.get('Bytes_sent'))/1024/1024) + 'M',
			        "Disk_temporary_tables":"%.2f%%" % (float(status_dict.get('Created_tmp_disk_tables')) / (float(status_dict.get('Created_tmp_tables')) + float(status_dict.get('Created_tmp_disk_tables'))) * 100),
			        "Thread_cache_hit_rate":"%.2f%%" % (float(status_dict.get('Threads_created')) / float(status_dict.get('Connections')) * 100),
			        "Table_cache_hit_rate":"%.2f%%" % (float(status_dict.get('Table_open_cache_hits')) / (float(status_dict.get('Table_open_cache_hits')) + float(status_dict.get('Table_open_cache_misses'))) * 100),
			        "Open_file_limit_used":"%.2f%%" % (float(status_dict.get('Open_files')) / float(variables_dict.get('open_files_limit')) * 100),
			        "Sort_temporary_tables":"%.2f%%" % (float(status_dict.get('Sort_merge_passes')) / (float(status_dict.get('Sort_scan')) + float(status_dict.get('Sort_range'))) * 100),
			        "joins_without_indexes":int(status_dict.get('Select_range_check')) + int(status_dict.get('Select_full_join')),
			        #"binlog_disk_cache":"%.2f%%" % (float(status_dict.get('Binlog_cache_disk_use')) / float(status_dict.get('Binlog_cache_use')) * 100),
			        "innodb_buffer_pool_size": int(variables_dict.get('innodb_buffer_pool_size')) /1024/1024/1024,
			        
			        
			        
			        
			        
			        
			        
			}
			data = json.dumps(audit_list)
			conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
			conn.select_db('release_system')
			cursor = conn.cursor()
			sql = "insert mysql_audit(audit) values('%s');" %(MySQLdb.escape_string(data))
			cursor.execute(sql)				

		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
			conn.close()
		return data



class MySQLDump(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_all_arguments()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		
		user = args.get('user')
		password = args.get('password')
		host = args.get('ip')
		port = int(args.get('port'))
		sql = args.get('sql')
		path = args.get('path')
		terminated = args.get('terminated')
					
		try:
			sql = "%s into outfile '%s' fields terminated by '%s'" %(sql, path, terminated)
			conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,charset="utf8")
			cursor = conn.cursor()
			cursor.execute(sql)
			rows = cursor.fetchall()
			data = json.dumps(rows, cls=DateEncoder)

		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
			conn.close()
		return data

class SQLCheck(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_textplain_argument()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		try:
			ddl_sql_text = []
			dml_sql_text = []
			init_sql_text = []
			user = "xxx"
			password = "xxx"
			operate = "check"
			
			projectversion =args.get('projectversion')
			host = args.get('dbip')
			port = args.get('dbport')
			db = args.get('dbname')
			sql_text = args.get('sqls')
			
			for i in sql_text:
				if re.search( r'ddl(.*).sql', i.get('name')):
					ddl_sql_text.append(i.get('sql'))
				elif re.search( r'dml(.*).sql', i.get('name')):
					dml_sql_text.append(i.get('sql'))
				elif re.search( r'init(.*).sql', i.get('name')):
					init_sql_text.append(i.get('sql'))
					
			
			if  len(ddl_sql_text):
				for sql in ddl_sql_text:
					sql = sql.replace('\"', '\'')
					sql = sql.replace('`', '\`')					
					cmd = 'ddl_manager -h %s -d %s  -P %s -a "%s" -j 1' %(host,db,port,sql)
					logging.info('sql_check:'+'%s' % cmd)
					res = create_process(cmd)
					data = list(res)
					log_path = res[1]
					log_path = ''.join(log_path.split('\n'))
					_log = open(log_path, "r")
					_list = []
					for line in _log.readlines():
						line = line.strip()
						_list.append(line)
					_log.close()
					data.append(_list)
					if data[0] ==0:
						data = {"code":data[0],"log_path":data[1],"log_message":' '.join(data[2][-1].split(' ')[4:])}
					else:
						data = {"code":data[0],"log_path":data[1],"log_message":' '.join(data[2][-1].split(' ')[4:])}
						
					data = json.dumps(data)
					
					
				
				
					
	
			
			try:
				conn = dba_opers.get_mysql_connection(options.inception_host, options.inception_user, options.inception_passwd, options.inception_port, True)
				cursor = conn.cursor()
				sql='''/*--user=%s;--password=%s;--host=%s;--enable-%s;--port=%s;*/
		
						                            inception_magic_start;
						                            set names utf8;
						                            use %s;
						                            %s
						                            inception_magic_commit;'''            
				#cursor.execute('SET NAMES utf8;')
				cursor.execute(sql % (user,password,host,operate,port,db,sql_text))
				result = cursor.fetchall()
				cursor.close()
				data = json.dumps(result)
			except:
				logging.error(traceback.format_exc())
		except:
			logging.error(traceback.format_exc())
		finally:
				
			cursor.close()
		return data

class QPS(BaseHandler):

	executor = ThreadPoolExecutor(3)

	@asynchronous
	@coroutine
	def post(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header("Access-Control-Allow-Headers", "x-requested-with")
		self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')            
		args = self.get_textplain_argument()
		ret = yield self.do(args)
		self.finish(ret)

	@run_on_executor
	def do(self, args):
		
		#user = 'xxx'
		#password = 'xxx'
		ip = args.get('ip')
		port = args.get('port')
		instance = ip + '_' + str(port)
		try:
			_list = []
			conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
			cursor = conn.cursor()
			cursor.execute("SELECT audit_time, JSON_EXTRACT(mysql_audit.audit,'$.QPS'),JSON_EXTRACT(mysql_audit.audit,'$.TPS')  FROM release_system.mysql_audit where instance = '%s';"% instance)
			rows = cursor.fetchall()
			#data = json.dumps(rows, cls=DateEncoder)
			for row in rows:
				time = row[0].strftime('%Y.%m.%d %H:%M')
				qps = row[1]
				tps = row[2]
				_list.append({"time": time,"qps": qps,"tps": tps})
			data = json.dumps(_list)			
			
		except:
			logging.error(traceback.format_exc())
		finally:
			cursor.close()
			conn.close()
		return data
