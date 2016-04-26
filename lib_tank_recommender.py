# -*- coding: utf-8 -*-

from py_utility.system import get_content_list
from extract_news_kws_lib import extract_news_kws
from config import CONNECTION
import time
import requests
import pynlpir
import json
import re
import copy


def white_list_test(url, white_list):
    white_list = get_content_list(white_list)
    flag = False
    for white_url in white_list:
        if url.find(white_url) > -1:
            flag = True
            break
    return flag


def url_filter(json_obj, white_list_file):
    filted_dataset = list()
    for item in json_obj:
        if white_list_test(item["Url"], white_list_file):
            filted_dataset.append({"title": item["Title"], "content": item["Content"], "iG": item["iG"]})
    return filted_dataset


def query_search_result(_news_title):
    num = None
    url = "http://news.baidu.com/ns"
    ct = time.time()
    secs_per_day = 3600*33
    bt = ct - secs_per_day
    et = ct
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'no-cache',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.52 Safari/537.36',
        'Host': 'news.baidu.com'
    }
    paras = {'word': _news_title,
             'bt': bt,
             'et': et,
             'ct': '1',
             'rn': '20',
             'ie': 'utf-8'}
    for i in range(CONNECTION.BAIDU_RETRY):   # Either request failed or succeed, the crawler sleep for 0.1 s, loop breaks when crawler succeed
        try:
            r = requests.get(url, params=paras, timeout=CONNECTION.BAIDU_TIMEOUT)
            print(r.url)
            html_body = r.text
            pattern = r'<span class="nums">.+?([0-9]*?,?[0-9]*?).</span>'
            match = re.findall(pattern, html_body)
            if len(match) > 0:   # pattern found, request succeed !
                num = int(match[0].replace(",", ""))
                if _news_title.find("!!!") > 0:
                    num += 999999999999
                time.sleep(0.1)
                break
            else:  # pattern not found, request failed, crawler retry this request
                print("Error Page Returned")
                time.sleep(0.1)
                continue
        except:
            print("Baidu Timeout")
            continue

    return num


def iG_tagging(iG):
    iG_obj = json.loads(iG)
    iG_list = iG_obj["s"]
    iG_list.sort(key=lambda ele: ele["w"], reverse=True)
    channel_tagging = iG_list[0]["n"]
    return channel_tagging


def hot_news_selector(clusters):
    hot_list = list()
    for item in clusters:
        title = item[0]
        kws = extract_news_kws(title)
        if len(title) > 15:
            hot_list.append([title, kws])
        else:
            continue
    print("Hot News Selection Complete")
    top_10 = hot_list[:10]
    return top_10


class DerivativeClustering:
    def __init__(self, item, _threshold):
        self.clusters = list()
        self.excludes = list()
        self.file_contents = [ele["content"] for ele in item]
        self.threshold = _threshold
        # self.files = [ele["title"]+" "+str(ele["num"]) for ele in item]
        self.files = [ele["title"] for ele in item]

    def test(self, file_x, file_y):
        file_path = [file_x, file_y]
        similarity_index = cal_similarity(file_path[0], file_path[1])
        if similarity_index > self.threshold:
            result = True
        else:
            result = False
        return result

    def find_derivative(self, current_index):
        files = self.file_contents
        cluster = [self.files[current_index]]
        if current_index not in self.excludes:
            for i in range(current_index, (len(files)-1)):
                if (i+1) not in self.excludes:
                    if self.test(files[current_index], files[i+1]):
                        cluster.append(self.files[i+1])
                        self.excludes.append(i+1)
        return cluster

    def clustering(self):
        for i in range(len(self.files)-1):
            if i not in self.excludes:
                self.clusters.append(self.find_derivative(i))
                print("Derivative Clustering Ieration: " + str(i))
        self.clusters.sort(key=lambda x: len(x), reverse=True)
        return self.clusters


def extract_news_kws(hot_news):
    pynlpir.open()
    s = hot_news
    kw_list = pynlpir.segment(s, pos_tagging=True, pos_names=None)
    kws = ""
    for kw in kw_list:
        pos = kw[0]
        tagging = kw[1]
        try:
            if tagging:
                # test if tagging is none, which means the pos is a space character
                tagging_first = tagging[0]
            else:
                tagging_first = ""
        except:
            tagging_first = ""
        if (tagging_first in "nsfvaz") and len(pos) > 1:
            if pos != "quot":
                kws = kws + pos + u" "
    kws = kws.strip(u" ")
    return kws


class TfParser(object):
    def __init__(self, content, norm="l1_norm"):
        self.norm = norm
        pynlpir.open()
        words = pynlpir.segment(content, pos_tagging=True, pos_names=None)

        kws = ""
        for word in words:
            pos = word[0]
            tagging = word[1]
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
        result = kws.split(" ")
        self.PoS = result

    def __call__(self):
        vector_space = set(self.PoS)
        tf_vector = {element: 0 for element in vector_space}
        for pos in self.PoS:
            if pos in vector_space:
                tf_vector[pos] += 1
        return vector_space, tf_vector


def tf_vec_normalization(vec, norm="l1_norm"):
    if norm == "l1_norm":
        sumation = sum(vec)
        new_vec = [ele/sumation for ele in vec]
    else:
        print("Wrong Argument Provided !")
        raise BaseException
    return new_vec


def cosine(vector_a, vector_b):
    def l2_norm(vector):
        norm_squared = 0
        for ele in vector:
            norm_squared += ele**2
        norm = norm_squared**0.5
        return norm

    l2_norm_a = l2_norm(vector_a)
    l2_norm_b = l2_norm(vector_b)
    dot_product = sum([vector_a[i]*vector_b[i] for i in range(len(vector_a))])
    cosine = dot_product/(l2_norm_a*l2_norm_b)
    return cosine


def tf_vector(content):
    parser = TfParser(content)
    return parser()[0], parser()[1]


def cal_similarity(file_a, file_b):
    vector_space_a, tf_a = tf_vector(file_a)
    vector_space_b, tf_b = tf_vector(file_b)
    vector_space = vector_space_a | vector_space_b
    vector_ele_pos_mapping = list(vector_space)
    vector_a = [0 for ele in range(len(vector_ele_pos_mapping))]
    vector_b = list(vector_a)
    for key in tf_a.keys():
        position = vector_ele_pos_mapping.index(key)
        vector_a[position] = tf_a[key]
    for key in tf_b.keys():
        position = vector_ele_pos_mapping.index(key)
        vector_b[position] = tf_b[key]
    cos = cosine(vector_a, vector_b)
    return cos

if __name__ == "__main__":
    """
    file_white_list = r"data/white_list.txt"
    file_path = r"data/filtered_news_from_yangzhuan.txt"
    json_file = r"CZD.json.patch1"
    generate_title_only_corpos(json_file, file_white_list)
    """

    file_white_list = r"data/white_list.txt"
    url = u'http://news.qq.com/a/20150915/006775.htm'
    # item_obj = url_filter(dataset, file_white_list)
    # result = white_list_test(url, file_white_list)
    # print(len(result))
