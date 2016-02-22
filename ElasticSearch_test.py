# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 11:48:16 2015

@author: Taikor
"""

import json
import requests
from py_utility import system
import time


def pull_news_from_tank(_start_index, _batch_size):
    url = "http://tank.palaspom.com:9200/palas_v1/_search"
    query = {"query":{"bool":{"must":[{"query_string":{"default_field":"items.analyzeData.issueID","query":"Reader"}},{"range":{"items.pubDate":{"from":"2015-09-12T00:00:01+08:00","to":"2015-09-12T23:59:59+08:00"}}}],"must_not":[],"should":[]}},"from":_start_index,"size":_batch_size,"sort":[],"facets":{}}
    timer = system.RunningTimer()
    r = requests.post(url, data=json.dumps(query))
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
        if test_void(title) and test_void(text) and "iG" in item["_source"]["analyzeData"][1]:
            news.append({"Url": item["_source"]["url"], "Title": title, "Content": text})
    return signal, news

file_path = r"2015-9-12.txt"
news_text = ""
news = list()
start_index = 0
es_batch_size = 100
while True:
    signal, news_buffer = pull_news_from_tank(start_index, es_batch_size)
    time.sleep(2)
    print("Batch Arrived")
    news = news + news_buffer
    if signal == -2:
        break
    start_index += es_batch_size

for record in news:
    news_text += record[0] + " " + record[1] + "\n"

with open(file_path, "w", encoding="utf8") as f:
    f.write(news_text)
