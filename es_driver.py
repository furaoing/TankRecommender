# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 11:48:16 2015

@author: Taikor
"""

import json
import requests
from py_utility import system
import time
from config import es_url
from config import construct_query
from config import es_tank_timeout
from config import es_batch_size
from config import ERROR


def pull_news_from_tank(_start_index, _batch_size, bt, et):
    signal = None
    url = es_url
    query = construct_query(bt, et, _start_index, _batch_size)
    timer = system.RunningTimer()
    r = requests.post(url, data=json.dumps(query), timeout=es_tank_timeout)

    print(timer.end())
    obj = json.loads(r.text)
    batch_size = len(obj["hits"]["hits"])
    if batch_size < _batch_size:
        signal = -2
    else:
        signal = 0
    
    news = list()
    
    def test_void(item_attr):
        if item_attr == "" or item_attr == None or item_attr == " ":
            return False
        else:
            return True
            
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
            news.append({"Url": item["_source"]["url"], "Title": title, "Content": text, "iG": ig})
    return signal, news

"""
def fetch_news_from_es(bt, et):
    news = list()
    start_index = 0
    while True:
        while True:
            try:
                signal, news_buffer = pull_news_from_tank(start_index, es_batch_size, bt, et)
                break
            except:
                print("Time Out")
                print("Try One More Time")
            time.sleep(10)
        time.sleep(1)
        print("Batch Arrived")
        news = news + news_buffer
        if signal == -2:
            break
        start_index += es_batch_size
    return news
"""


def fetch_news_from_es(bt, et):
    news = list()
    start_index = 0
    signal = None
    news_buffer = None

    while True:
        # starting fetching data from es on a batch by batch style
        for attempt in range(10):
            try:
                signal, news_buffer = pull_news_from_tank(start_index,
                                                          es_batch_size, bt, et)
            except:
                print("Time Out")
                print("Try One More Time")
                time.sleep(5)
                # sleep for 5 sec and re-try

                signal = ERROR.PULL_METHOD_FAILED
                # signal = -3 indicates that pull new method failed

                continue
                # move to the next attempt
            break

        if signal is None:
            # Terminate execuation is signal is None
            print("Error, var signal is not assigned value")
            raise Exception

        if signal == ERROR.ES_NO_MORE_DATA:
            break
            # no more batches based on query
        elif signal == ERROR.PULL_METHOD_FAILED:
            print("All retry failed, break out of loop")
            break
        elif signal == ERROR.ES_HAS_DATA:
            print("Batch Arrived")

            if news_buffer:
                news = news + news_buffer
            else:
                raise Exception
            start_index += es_batch_size
            # es_batch_size is a global variable in the current scope

            time.sleep(2)
            # sleep for 2 sec and then fetch the next batch

        else:
            print("Error, var signal is assigned illegal value")
            raise Exception

    return news
