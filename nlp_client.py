# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

import thriftpy

from thriftpy.rpc import client_context
from config import HANLPRPC_HOST
from config import HANLPRPC_PORT
from waffle import system
import time

rel_path = r"hanlprpc.thrift"
abs_path = system.create_abs_path(rel_path)
hanlprpc_thrift = thriftpy.load(abs_path,
                                module_name="hanlprpc_thrift")


def rpc_seg(text):
    result = ""
    with client_context(hanlprpc_thrift.Segmenter,
                        HANLPRPC_HOST, HANLPRPC_PORT) as client:

        result = client.seg(text)
    time.sleep(0.2)
    return result

if __name__ == '__main__':
    result = rpc_seg("中华人民共和国成立一百周年")

