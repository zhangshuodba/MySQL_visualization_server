from tornado.web import RequestHandler
import json
import traceback
import logging


class BaseHandler(RequestHandler):
    
    
    def get_all_arguments(self):
        request_params = {}
        args = self.request.arguments
        for key in args:
            request_params.setdefault(key, args[key][0])
        return request_params
    
    def get_textplain_argument(self):
        conten_tpye = self.request.headers.get('Content-Type').split(';')[0]
        if conten_tpye in ['text/plain']:
            try:
                request_params = json.loads(self.request.body.decode('utf-8'))
                return request_params
            except:
                logging.error(traceback.format_exc())
        elif conten_tpye in ['application/json']:
            try:
                request_params = json.loads(self.request.body.decode('utf-8'))
                return request_params
            except:
                logging.error(traceback.format_exc())
        

    
