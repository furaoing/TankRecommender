# -*- coding: utf-8 -*-

import pynlpir
from nlp_client import rpc_seg


def extract_news_kws(hot_news):
    s = hot_news
    kw_list = rpc_seg(s)
    kws = ""
    for kw in kw_list:
        pos = kw["word"]
        tagging = kw["nature"]
        try:
            if tagging:
                # test if tagging is none, which means the pos is a space character
                tagging_first = tagging[0]
            else:
                tagging_first = ""
        except:
            tagging_first = ""
        if tagging_first == "n" and len(pos) > 1:
            if pos != "quot":
                kws = kws + pos + u" "
    kws = kws.strip(u" ")
    return kws

if __name__ == "__main__":
    hot_news = '小微企业再获减税"礼包":所得税减半 年应税额扩..'
    print(extract_news_kws(hot_news))
