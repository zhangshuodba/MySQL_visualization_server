#-*- coding: utf-8 -*-

import os
import urllib
import json
import logging

from tornado.httpclient import HTTPRequest, HTTPClient


def _request_fetch(request):
    http_client = HTTPClient()
    
    response = None
    try:
        response = http_client.fetch(request)
    finally:
        http_client.close()
    
    return_result = False
    if response is None:
        raise('response is None!')
    
    if response.error:
        return_result = False
        message = "error message:%s" % (request,response.error)
        logging.error(message)
    else:
        return_result = response.body.strip()
            
    return return_result


def http_post(url, body={}, _connect_timeout=40.0, _request_timeout=40.0, auth_username=None, auth_password=None):
    request = HTTPRequest(url=url, method='POST', body=urllib.urlencode(body), connect_timeout=_connect_timeout, \
                          request_timeout=_request_timeout, auth_username = auth_username, auth_password = auth_password)
    fetch_ret = _request_fetch(request)
    return_dict = json.loads(fetch_ret)
    logging.info('POST result :%s' % str(return_dict))
    return return_dict


def http_get(url, _connect_timeout=40.0, _request_timeout=40.0, auth_username=None, auth_password=None):   
    request = HTTPRequest(url=url, method='GET', connect_timeout=_connect_timeout, request_timeout=_request_timeout,\
                          auth_username = auth_username, auth_password = auth_password)
    fetch_ret = _request_fetch(request)
    return_dict = json.loads(fetch_ret)
    logging.info('GET result :%s' % str(return_dict))
    return return_dict


def nc_ip_port_available(host_ip, port):
    cmd = 'nc -z -w1 %s %s' % (host_ip, port)
    _nc_ret = os.system(cmd)
    if _nc_ret != 0:
        return False
    return True