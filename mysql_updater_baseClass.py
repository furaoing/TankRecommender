# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, VARCHAR, DATETIME, Binary
from sqlalchemy.orm import sessionmaker
import time

from config import DB_HOST
from config import DB_USER
from config import DB_PASSWORD
from config import DB_DATABASE
from config import DB_TYPE
from config import DB_CONNECTOR
from config import DB_CHARSET
from config import HeatTopic


class MySQLUpdater(object):
        def __init__(self):
            self.counter = 0
            self.con_str = "%s+%s://%s:%s@%s/%s?charset=%s" % \
                                     (
                                         DB_TYPE,
                                         DB_CONNECTOR,
                                         DB_USER,
                                         DB_PASSWORD,
                                         DB_HOST,
                                         DB_DATABASE,
                                         DB_CHARSET
                                     )

            # 创建连接引擎
            engine = create_engine(self.con_str, echo=True)

            # 创建连接Session类
            DBSession = sessionmaker(bind=engine)

            # 由Session类实例化一个session
            self.session = DBSession()

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

            topic = HeatTopic(HeatTopicID=id,
                              HeatTopicName=topic_title_and_kw[0],
                              IssueID="Reader",
                              QueryRule=topic_title_and_kw[1],
                              CreateTime=formated_time
                              )

            self.session.add(topic)
            self.session.commit()
            self.counter += 1

        def clean_up(self):
            self.session.close()

if __name__ == "__main__":

    con_str = "%s+%s://%s:%s@%s/%s?charset=%s" % (DB_TYPE,
                                                  DB_CONNECTOR,
                                                  DB_USER,
                                                  DB_PASSWORD,
                                                  DB_HOST,
                                                  DB_DATABASE,
                                                  DB_CHARSET)


    # 创建MYSQL Table的基类:
    Base = declarative_base()

    # 创建连接引擎
    engine = create_engine(con_str, echo=True)

    # 创建连接Session类
    DBSession = sessionmaker(bind=engine)



    # 由Session类实例化一个session
    session = DBSession()
    topic = HeatTopic(HeatTopicID="111",
                              HeatTopicName="111",
                              IssueID="Reader",
                              QueryRule="111",
                              )
    session.add(topic)
    session.commit()
    session.close()


