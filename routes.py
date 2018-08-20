from handlers.server import *



handlers = [
    
    ##'''  -------------------- server regedit-----------------------------
    (r"/server/admin", AdminServerHandler),
    (r"/server/processList", processList),
    (r"/server/list", ShowServerListHandler),
    (r"/server/inception", InceptionHandler),
    (r"/server/dbinfo", dbInfo),
    (r"/server/adddbinfo", AddDBInfo),
    (r"/server/historysql", historySQL),
    (r"/server/submitedsqldetail", SubmitedSQLDetail), 
    (r"/server/kill", Kill), 
    (r"/server/slowlog", SlowLog),
    (r"/server/binlog", BinLog),
    (r"/server/ddl", DDL),
    (r"/server/sqlexcute", SQLEXCUTE),
    (r"/server/mysqlaudit", MySQLAudit),
    (r"/server/mysqldump", MySQLDump),
    (r"/server/qps", QPS),
    (r"/login", Login), 
    ##'''  -------------------- nerv-----------------------------
    (r"/sqlservice/check", SQLCheck),
    
    
    
]

