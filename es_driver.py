# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 11:48:16 2015

@author: Taikor
"""

import json
import requests
from waffle import system
import time
from retry import retry
from config import ES_URL
from config import construct_query
from config import ES_TANK_TIMEOUT
from config import ES_BATCH_SIZE
from config import ERROR


@retry(tries=3, delay=2)
def es_fetch(query):
    return requests.post(ES_URL,
                         data=json.dumps(query),
                         timeout=ES_TANK_TIMEOUT)


def pull_news_from_tank(_start_index, _batch_size, bt, et):

    def test_void(item_attr):
        if item_attr == "" or item_attr == None or item_attr == " ":
            return False
        else:
            return True

    news = list()
    query = construct_query(bt, et, _start_index, _batch_size)
    timer = system.Timer()
    r = es_fetch(query)
    t = str(timer.end())
    print("ES Query Time: " + t)
    obj = json.loads(r.text)
    batch_size = len(obj["hits"]["hits"])
    if batch_size < _batch_size:
        signal = ERROR.ES_NO_MORE_DATA
    else:
        signal = ERROR.ES_HAS_DATA
            
    for item in obj["hits"]["hits"]:
        if "cleanTitle" in item["_source"]:
            title = item["_source"]["cleanTitle"]
        else:
            title = None
        if "cleanText" in item["_source"]:
            text = item["_source"]["cleanText"]
        else:
            text = None

        ig = None
        for ele in item["_source"]["analyzeData"]:
            if "iG" in ele:
                ig = ele["iG"]
                break

        if test_void(title) and test_void(text) and ig:
            news.append({"Url": item["_source"]["url"],
                         "Title": title,
                         "Content": text,
                         "iG": ig})
    return signal, news


def fetch_news_from_es(bt, et):
    news = list()
    start_index = 0

    while True:
        # starting fetching data from es on a batch by batch style
        try:
            signal, news_buffer = pull_news_from_tank(start_index,
                                                      ES_BATCH_SIZE, bt, et)

            if signal is None:
                # Terminate execuation if signal is None
                print("Error, var signal is not assigned value")
                raise Exception

            if signal == ERROR.ES_NO_MORE_DATA:
                break
                # no more batches based on query
            elif signal == ERROR.ES_HAS_DATA:
                print("Batch Arrived")

                news = news + news_buffer
                start_index += ES_BATCH_SIZE
                time.sleep(2)
                # sleep for 2 sec and then fetch the next batch
            else:
                print("Error, var signal is assigned illegal value")
                raise Exception

        except Exception:
            print("Retries failed, break out of loop, drop ES connection attempts")
            break
    return news
