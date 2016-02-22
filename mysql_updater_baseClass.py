# -*- coding: utf-8 -*-

import MySQLdb
import time
from config import MYSQL_HOST
from config import MYSQL_USER
from config import MYSQL_PASSWD
from config import MYSQL_DB


class MySQLUpdater(object):
    def __init__(self):
        self.db = MySQLdb.Connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            passwd=MYSQL_PASSWD,
            db=MYSQL_DB,
        )
        self.db.query('SET NAMES utf8')
        self.cursor = self.db.cursor()
        self.counter = 0

    def get_formated_time(self):
        struct_time = time.localtime()
        year = struct_time.tm_year
        month = struct_time.tm_mon
        day = struct_time.tm_mday
        hour = struct_time.tm_hour
        min = struct_time.tm_min
        sec = struct_time.tm_sec
        formated_time = str(year) + "-" + str(month) + "-" + str(day) + " " + str(hour) + ":" + str(min) + ":" + str(sec)
        return formated_time, int(time.time())

    def query(self, topic_title_and_kw):
        topic_title_and_kw[1] = topic_title_and_kw[1].replace(" ", "+")
        topic_title_and_kw[0] = topic_title_and_kw[0].strip("!!!")
        formated_time, time_stamp = self.get_formated_time()
        id = self.counter + time_stamp
        sql = r"INSERT INTO HeatTopic (HeatTopicID, HeatTopicName, IssueID, QueryRule, CreateTime) VALUES ('%s', '%s', '%s', '%s', '%s')" % (id, topic_title_and_kw[0], "Reader", topic_title_and_kw[1], formated_time)
        sql = sql.encode('utf8')
        query_msg = self.cursor.execute(sql)
        self.db.commit()
        self.counter += 1
        return query_msg

    def clean_up(self):
        self.db.close()



