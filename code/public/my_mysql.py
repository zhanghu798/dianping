#coding=utf8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import time

import MySQLdb

class mysql_class(object):
    """
    自动管理mysql连接.继承自object, 可以自动回收垃圾
    """
    #def __init__(self, host='115.28.77.226', user='glz007', passwd='glzdb', charset='utf8', db = "zhangh"):
    def __init__(self, host='127.0.0.1', user='root', passwd='think', db="proxy", charset='utf8', ):
        self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset="utf8")
        self.cursor = self.conn.cursor()

    def get_conn_and_cursor(self):
        return self.conn, self.cursor

    def execute(self, sql_order, logger, auto_commit=True):
        try:
            ret = self.cursor.execute(sql_order)
            if auto_commit:
                self.conn.commit()
            return ret
        except Exception as e:
            logger.error(e)
            self.conn.rollback()
            with open('wrong.sql', 'w') as f:
                f.write(sql_order)
            return -1
        return 1

    def insert(self, sql_order):
        self.cursor.execute(sql_order)
        id = self.conn.insert_id()
        self.conn.commit()
        return id

    def __del__(self):
        self.cursor.close()
        self.conn.close()
