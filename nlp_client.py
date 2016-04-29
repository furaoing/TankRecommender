# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

import thriftpy

from thriftpy.rpc import client_context
from config import HANLPRPC_HOST
from config import HANLPRPC_PORT

hanlprpc_thrift = thriftpy.load(r"hanlprpc.thrift",
                                module_name="hanlprpc_thrift")


def rpc_seg(text):
    result = ""
    with client_context(hanlprpc_thrift.Segmenter,
                        HANLPRPC_HOST, HANLPRPC_PORT) as client:

        result = client.seg(text)
    return result

if __name__ == '__main__':
    result = rpc_seg("中华人民共和国成立一百周年")

