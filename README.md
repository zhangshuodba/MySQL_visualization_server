## 当前版本：v1.0.0

`注：基于tornado框架开发`

## 技术栈

python2.7 + tornado + jwt + MySQL

## run
```bush
python ops_server.py
```

## 一个功能类实现解析

```bush

class Example(BaseHandler):

	executor = ThreadPoolExecutor(3)  #初始化线程池

	@asynchronous  #长连接和短连接相关
	@coroutine  #调用协成装饰器
	def post(self):
		args = self.get_all_arguments()
		ret = yield self.do(args)  #yield执行时记录当前环境，执行完成的流过来后再把记录加载进来
		self.finish({'message': ret})  #关闭连接并返回结果

	@run_on_executor  #调用装饰器把耗时任务丢到线程池操作
	def do(self, args):
		try:
			pass

		except:
			logging.error(traceback.format_exc())
		finally:
			pass
		return 'success'
```

		
## 基于JWT做无状态秘钥签发

```bush

encoded = jwt.encode({
        'user_name': _list[0].get('user_name'),
        'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=600)},
                        SECRET_KEY,
                        algorithm='HS256'
                        )
response = {'token':encoded.decode('ascii')}

{'token': u'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX25hbWUiOiJkYmEiLCJleHAiOjE1MzQ4NDA1MzB9.jkKuHNg098wEEZbQnY81f0UgimhqOWgPmTE1rqZDubI'}

```

## 基于JWT做无状态秘钥验证:

```bush

jwt.decode(
                token,
                SECRET_KEY,
                options=jwt_options
        )
```


## 文件结构
```shell
.
├── appconfig
│   ├── parse_config.py  配置文件
├── config
│   └── logging.conf  日志配置
├── handlers
│   ├── base.py
│   ├── server.py  一个功能对应一个类实现
├── opers
│   ├── dba_opers.py
├── ops_server.py  启动程序
├── routes.py 路由
├── scheduler 定时器
├── utils  工具
└── var 日志
```

## License
[MIT](http://opensource.org/licenses/MIT)