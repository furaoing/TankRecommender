# -*- coding: utf-8 -*-

from lib_tank_recommender import query_search_result
from lib_tank_recommender import url_filter
from lib_tank_recommender import hot_news_selector
from es_driver import fetch_news_from_es
from lib_tank_recommender import DerivativeClustering
from mysql_updater_baseClass import MySQLUpdater
from waffle.system import f_write
from waffle.system import create_abs_path
from config import RESULT_LOG_BaseName
from config import CLUSTERS_LOG_BaseName
from config import get_log_file_pth
from config import UPDATE_TIME_POINT
from config import FILE_WHITE_LIST
from config import DEBUG_BT
from config import DEBUG_ET
from config import is_debug
import json
import time


class HotNewsDiscoverAgent(object):
    def __init__(self, _times_to_update, debug=False):
        """
          using attribute debug to switch mode, when debug=True, the hot news
            generating will immediately started regardless what times_to_update
            is set to
        """
        self.times_to_update = _times_to_update
        self.debug = debug

    def update_db_when_time_is_right(self):
        result = self.time_match(self.times_to_update)

        if self.debug:
            self.update_db(DEBUG_BT, DEBUG_ET)
        if result:
            bt, et = self.get_bt_et(result)
            self.update_db(bt, et)
            return 0
        else:
            return -1

    def run(self):
        while True:
            try:
                return_code = self.update_db_when_time_is_right()
                if return_code == 0:
                    time.sleep(61)
            except Exception as e:
                pass
            time.sleep(1)

    def time_match(self, times):
        ct = time.localtime()
        hour = ct.tm_hour
        min = ct.tm_min
        result = False
        for time_to_update in times:
            if hour == time_to_update["hour"] and min == time_to_update["min"]:
                result = {"hour": hour, "min": min}
                break
            else:
                result = False
        return result

    def get_bt_et(self, _result):
        ct = time.localtime()
        year = ct.tm_year
        month = ct.tm_mon
        day = ct.tm_mday
        sec = ct.tm_sec

        end_hour_min = _result
        if end_hour_min == self.times_to_update[0]:
            begin_hour_min = self.times_to_update[1]
        else:
            begin_hour_min = self.times_to_update[0]

        if end_hour_min["hour"] < begin_hour_min["hour"]:
            begin_day = day - 1
            begin_hour = begin_hour_min["hour"]
            begin_min = begin_hour_min["min"]
            end_day = day
            end_hour = end_hour_min["hour"]
            end_min = end_hour_min["min"]
        else:
            begin_day = day
            begin_hour = begin_hour_min["hour"]
            begin_min = begin_hour_min["min"]
            end_day = day
            end_hour = end_hour_min["hour"]
            end_min = end_hour_min["min"]

        bt = str(year) + "-" + str(month) + "-" + str(begin_day) + "T" + str(begin_hour) + ":" + str(begin_min) + ":" + str(sec) + "+08:00"
        et = str(year) + "-" + str(month) + "-" + str(end_day) + "T" + str(end_hour) + ":" + str(end_min) + ":" + str(sec) + "+08:00"
        return bt, et

    def update_db(self, bt, et):
        file_white_list = create_abs_path(FILE_WHITE_LIST)
        news_dataset = fetch_news_from_es(bt, et)
        print("All data: " + str(len(news_dataset)))
        item_obj = url_filter(news_dataset, file_white_list)
        print("Filted data: " + str(len(item_obj)))

        title_result = list()
        for item in item_obj:
            # num = query_search_result(item["title"])
            num = 1
            if num:
                # if num is not None, push data to title_result
                title_result.append({"title": item["title"], "content": item["content"],
                                     "num": num, "iG": item["iG"]})
            else:
                # if num is None , skip this kw
                continue

        title_result.sort(key=lambda title_r: title_r["num"], reverse=True)
        sorted_title = title_result[:200]

        result_hot = sorted_title
        threshold = 0.2
        d = DerivativeClustering(result_hot, threshold)
        clusters = d.clustering()

        test_buffer = ["\n".join(item) for item in clusters]
        test_str = "\n\n".join(test_buffer)

        test_result_pth = get_log_file_pth(RESULT_LOG_BaseName)
        f_write(test_result_pth, test_str)
        hot_news_and_kws = hot_news_selector(clusters)

        mysql_updater = MySQLUpdater()
        for item in hot_news_and_kws:
            mysql_updater.query(item)
        mysql_updater.clean_up()
        print("Db Updated !")
        print("\n")
        print("Clusters:")
        print(clusters)
        print("\n")
        print("Hot News:")
        print(hot_news_and_kws)

        test_clusters_pth = get_log_file_pth(CLUSTERS_LOG_BaseName)
        json_str = json.dumps(clusters, ensure_ascii=False)
        f_write(test_clusters_pth, json_str)
        return 0


if __name__ == "__main__":
    agent = HotNewsDiscoverAgent(UPDATE_TIME_POINT, debug=is_debug)
    agent.run()
