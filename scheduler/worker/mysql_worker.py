
import logging
import traceback
import threading
import MySQLdb
import json

from .base_worker import BaseWorker
from tornado.options import options
from opers.dba_opers import dba_opers



class MysqlAudit(BaseWorker):
    #first_init_qps = 0
    #first_init_tps = 0
    user = 'cathy'
    password = 'cathy'
    host = '100.73.20.3'
    port = 13307    
    
    #initial value
    sql_status = "show global status"
    status_name = []
    status_value = []
    conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,charset="utf8")
    cursor = conn.cursor()
    cursor.execute(sql_status)  
    sql_status = cursor.fetchall()
    for row in sql_status:
        status_name.append(row[0])
        status_value.append(row[1])
    status_dict = dict(zip(status_name, status_value))
    first_init_tps = int(status_dict.get('Com_insert')) + int(status_dict.get('Com_update')) + int(status_dict.get('Com_delete'))
    first_init_qps = int(status_dict.get('Com_select')) + int(status_dict.get('Com_insert')) + int(status_dict.get('Com_update')) + int(status_dict.get('Com_delete')) + int(status_dict.get('Com_replace'))
    
    
    

    def __init__(self,timeout=5):
        self.timeout = timeout
      

        
    def mysqlaudit(self):
        logging.info('do mysqlaudit')
    
        user = 'cathy'
        password = 'cathy'
        host = '100.73.20.3'
        port = 13307
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
            

            now_tps = int(status_dict.get('Com_insert')) + int(status_dict.get('Com_update')) + int(status_dict.get('Com_delete'))
            now_qps = int(status_dict.get('Com_select')) + int(status_dict.get('Com_insert')) + int(status_dict.get('Com_update')) + int(status_dict.get('Com_delete')) + int(status_dict.get('Com_replace'))
            
            
            
            
            audit_list = {
                "Uptime":status_dict.get('Uptime'),
                "QPS":now_qps - MysqlAudit.first_init_qps,
                "TPS":now_tps - MysqlAudit.first_init_tps,
                #"QPS":int(status_dict.get('Com_select')) + int(status_dict.get('Com_insert')) + int(status_dict.get('Com_update')) + int(status_dict.get('Com_delete')) + int(status_dict.get('Com_replace')),
                #"TPS":int(status_dict.get('Com_insert')) + int(status_dict.get('Com_update')) + int(status_dict.get('Com_delete')),
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
            MysqlAudit.first_init_qps = now_qps
            MysqlAudit.first_init_tps = now_tps      
            data = json.dumps(audit_list)
            conn = dba_opers.get_mysql_connection(options.mysql_host, options.mysql_user, options.mysql_passwd, options.mysql_port, True)
            conn.select_db('release_system')
            cursor = conn.cursor()
            sql = "insert mysql_audit(audit,instance) values('%s','%s');" %(MySQLdb.escape_string(data),host + '_' + str(port))
            cursor.execute(sql)				
    
        except:
            logging.error(traceback.format_exc())
        finally:
            cursor.close()
            conn.close()    

        
        
