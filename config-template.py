import time
from py_utility import system

RESULT_LOG_BaseName = r"log/result"
CLUSTERS_LOG_BaseName = r"log/clusters"
times_to_update = [{"hour": 10, "min": 18}, {"hour": 16, "min": 27}]
es_url = ""
es_tank_timeout = 10
es_batch_size = 300
MYSQL_HOST = ""
MYSQL_USER = ""
MYSQL_PASSWD = ""
MYSQL_DB = ""
FILE_WHITE_LIST = r"data/white_list.txt"


class ERROR(object):
    PULL_METHOD_FAILED = -3
    ES_NO_MORE_DATA = -2
    ES_HAS_DATA = 0


class CONNECTION(object):
    BAIDU_TIMEOUT = 2
    BAIDU_RETRY = 2


def get_log_file_pth(base_name):
    time_date = system.get_formated_time(mode="log")
    relative_pth = base_name + time_date
    abs_path = system.create_abs_path(relative_pth)
    return abs_path


def construct_query(bt, et, _start_index, _batch_size):
    query = {"query":{"bool":{"must":[{"query_string":{"default_field":"items.analyzeData.issueID","query":"Reader"}},{"range":{"items.pubDate":{"from":bt,"to":et}}}],"must_not":[],"should":[]}},"from":_start_index,"size":_batch_size,"sort":[],"facets":{}}
    return query
