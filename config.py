import time
from waffle.util.datetime import TimeFormater
from waffle import system
from sqlalchemy import create_engine, Column, VARCHAR, DATETIME, Binary
from sqlalchemy.ext.declarative import declarative_base

""" Timer Setting """

UPDATE_TIME_POINT = [
    {
        "hour": 9,
        "min": 34
    },
    {
        "hour": 16,
        "min": 27
    }
]

""" External File and Logging File Relative Path """

RESULT_LOG_BaseName = r"log/result"
CLUSTERS_LOG_BaseName = r"log/clusters"
FILE_WHITE_LIST = r"data/white_list.txt"


""" Database IO Information """

ES_URL = "http://mech.palaspom.com:9200/palas_v1/_search"
ES_TANK_TIMEOUT = 10
ES_BATCH_SIZE = 300

DB_HOST = "www.pubtopic.org"
DB_USER = "palas"
DB_PASSWORD = "lapas"
DB_DATABASE = "Palas_V5"
DB_TYPE = "mysql"
DB_CONNECTOR = "pymysql"
DB_CHARSET = "utf8"

""" MySQL Table Structure """

# 创建数据表结构的基类:
Base = declarative_base()

# 创建映射库内HeatTopic数据表结构的类:


class HeatTopic(Base):
        # 表名称字符串
        __tablename__ = "HeatTopic"

        # 字段定义
        HeatTopicID = Column(VARCHAR(32), primary_key=True, nullable=False)
        HeatTopicName = Column(VARCHAR(100), default=None, nullable=True)
        IssueID = Column(VARCHAR(32), default=None, nullable=True)
        QueryRule = Column(VARCHAR(1000), default=None, nullable=True)
        CreateTime = Column(DATETIME, default=None, nullable=True)
        IsDisabled = Column(Binary(1), default=0, nullable=True)


""" ENUM Types """


class ERROR(object):
    PULL_METHOD_FAILED = -3
    ES_NO_MORE_DATA = -2
    ES_HAS_DATA = 0


class CONNECTION(object):
    BAIDU_TIMEOUT = 2
    BAIDU_RETRY = 2


""" Miscellaneous Functions """


def get_log_file_pth(base_name):
    t = TimeFormater("es")
    time_date = t.format_time()

    relative_pth = base_name + time_date
    abs_path = system.create_abs_path(relative_pth)
    return abs_path


def construct_query(bt, et, _start_index, _batch_size):
    query = {
        "query":
                 {
                     "bool":
                      {
                          "must":
                           [
                               {
                                   "query_string":
                                 {
                                     "default_field":  "items.analyzeData.issueID",
                                     "query":  "Reader"
                                 }
                             },
                            {
                                "range":
                                    {
                                "items.pubDate":
                                           {
                                               "from": bt,
                                               "to": et
                                            }
                                       }
                             }
                            ],
                          "must_not": [],
                          "should": []
                      }
                 },
        "from": _start_index,
        "size": _batch_size,
        "sort": [],
        "facets": {}
    }

    return query
