#coding=utf8

import os

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from chardet import detect
from bs4 import BeautifulSoup
import re
from MySQLdb import escape_string
import json
import datetime

G_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
G_CODE_DIR = os.path.dirname(G_CURRENT_DIR)
G_ROOT_DIR = os.path.dirname(G_CODE_DIR)


G_LOG_DIR = os.path.join(G_ROOT_DIR, 'logger')
G_OUT_DIR = os.path.join(G_ROOT_DIR, 'out')
for dir in [G_LOG_DIR, G_OUT_DIR]:
    if not os.path.exists(dir):
        os.mkdir(dir)


sys.path.append(G_ROOT_DIR)
sys.path.append(os.path.join(G_CODE_DIR, 'public'))


try:
    from ..public.logger import get_logger
    from ..public.my_mysql import mysql_class
    from ..public.spider import get_session
    from ..public.spider import download_use_session

except Exception as e:
    from logger import get_logger
    from my_mysql import mysql_class
    from spider import get_session
    from spider import download_use_session


"""
数据库建库命令:
CREATE DATABASE `dianping` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci
"""

class search_condition_class(object):
    """
    搜索条件
    """
    def __init__(self):
        self.province = ''
        self.city = ''
        self.project = ''               # 大项目种类, 美食, 休闲....
        self.category = ''              # 分类, https://www.dianping.com/search/category/2/10/r2578
        self.area = ''                  # 行政区域
        self.busi_area = ''             # 商圈
        self.other_search_con = ''      # 其他搜索条件
        self.search_url = ''            # 满足以上条件的搜索url
        self.shop_count = -1            # 商家个数
        self.download_datetime = ''     # 数据获取日期+时间
        self.data_status = 0            # 数据状态, -1: 数据失效, 0: 正常

        self.downloaded_shop_count = 0    # 该搜索条件下下载的店铺数据个数

        self.other = ''                 # 其他说明
        
    def display(self):
        print "province=@@%s@@" % self.province
        print "city=@@%s@@" % self.city
        print "project=@@%s@@" % self.project
        print "category=@@%s@@" % self.category
        print "area=@@%s@@" % self.area
        print "busi_area=@@%s@@" % self.busi_area
        print "other_search_con=@@%s@@" % self.other_search_con
        print "search_url=@@%s@@" % self.search_url
        print "shop_count=%d" % self.shop_count
        print "download_datetime=@@%s@@" % self.download_datetime
        print "data_status=@@%s@@" % self.data_status
        print "other=@@%s@@" % self.other
        print "downloaded_shop_count=@@%s@@" % self.downloaded_shop_count

    def create_table(self):
        """
         CREATE TABLE `tbl_search_condition` (
          `id` int(11) unsigned NOT NULL AUTO_INCREMENT,

          `project` varchar(30) DEFAULT '' COMMENT '大种类',
          `category` varchar(30) DEFAULT '' COMMENT '分类',
          `province` varchar(50) DEFAULT '' COMMENT '省份',
          `city` varchar(50) DEFAULT '' COMMENT '城市',
          `area` varchar(50) DEFAULT '' COMMENT '行政区域',
          `busi_area` varchar(50) DEFAULT '' COMMENT '商圈',
          `other_search_con` varchar(100) DEFAULT '' COMMENT '其他搜索条件',
          `search_url` varchar(150) DEFAULT '' COMMENT '搜索url',
          `shop_count` int(11) DEFAULT 0 COMMENT '商家个数',
          `other` varchar(100) DEFAULT '' COMMENT '其他说明',

          `download_datetime` datetime DEFAULT '00000-00-00 00:00:00' COMMENT '数据下载日期',
          `insert_datetime` datetime DEFAULT CURRENT_TIMESTAMP,
          `update_datetime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,

          `data_status` int(3) DEFAULT NULL COMMENT '信息状态；-1:该信息一经被删除',
          `downloaded_shop_count` int(11) DEFAULT 0 COMMENT '该搜索条件下下载的店铺数据个数',

          PRIMARY KEY (`id`),
          UNIQUE KEY `search_url` (`search_url`),
          UNIQUE KEY `search_condition` (`project`,`category`, `province`, `city`, `area`, `busi_area`, `other_search_con`)

        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='搜索条件表'
        """

    def storn_search_conditon(self, mysql_handle, logger):
        sql = 'select search_url from tbl_search_condition where search_url = "%s"' % self.search_url

        res = mysql_handle.execute(sql_order=sql, auto_commit=True, logger=logger)

        res_tuple = mysql_handle.cursor.fetchone()

        if res_tuple != None:
            return 1

        sql = 'insert into tbl_search_condition set ' +\
            'project="%s",' % self.project +\
            'category="%s",' % self.category +\
            'province="%s",' % self.province +\
            'city="%s",' % self.city +\
            'area="%s",' % self.area +\
            'busi_area="%s",' % self.busi_area +\
            'other_search_con="%s",' % self.other_search_con +\
            'search_url="%s",' % self.search_url +\
            'shop_count=%d,' % self.shop_count +\
            'other="%s",' % self.other +\
            'downloaded_shop_count=%d,' % self.downloaded_shop_count +\
            'download_datetime="%s" ' % self.download_datetime

        res = mysql_handle.execute(sql_order=sql, auto_commit=True, logger=logger)
        if res < 0:
            logger.error('res=%d' % res)
        return res

"""
alter table tbl_search_condition change business_count shop_count  int(11) DEFAULT '0' COMMENT '商家个数'
alter table tbl_search_condition add downloaded_shop_count  int(11) DEFAULT '0' COMMENT '已下载商家个数'

"""


class shop_info_class(object):
    """
    店铺信息
    """
    def __init__(self):
        self.project = ''
        self.category = ''
        self.province = ''
        self.city = ''
        self.area = ''
        self.busi_area = ''

        self.shope_name = ''
        self.shop_url = ''
        self.type = ''
        self.comment_count = -1
        self.consume_average_str = ''
        self.consume_average = -1
        self.taste_score = -1
        self.env_score = -1
        self.serve_score = -1
        self.addr = ''
        self.all_comment_count = -1
        self.default_comment_count = -1
        self.group_comment_count = -1
        self.short_comment_count = -1

        self.five_star_count = -1
        self.four_star_count = -1
        self.three_star_count = -1
        self.two_star_count = -1
        self.one_star_count = -1
        self.has_parking_count = -1
        self.has_pic_count = -1

        self.download_datetime = ''

        self.first_show_comment_date = ''
        self.last_show_comment_date = ''
        self.show_comment_count = -1

        self.tags = ''
        self.search_conditions = ''

        self.note = {}

    def display(self):
        print 'project=@@%s@@' % self.project
        print 'category=@@%s@@' % self.category
        print 'province=@@%s@@' % self.province
        print 'city=@@%s@@' % self.city
        print 'area=@@%s@@' % self.area
        print 'busi_area=@@%s@@' % self.busi_area

        print "shop_name=@@%s@@" % self.shope_name
        print 'shop_url=@@%s@@' % self.shop_url
        print 'type=@@%s@@' % self.type
        print 'comment_count=@@%d@@' % self.comment_count

        print 'consume_average_str=@@%s@@' % self.consume_average_str
        print 'consume_average=@@%f@@' % self.consume_average
        print 'taste_score=@@%f@@' % self.taste_score
        print 'env_score=@@%f@@' % self.env_score
        print 'serve_score=@@%f@@' % self.serve_score
        print 'addr=@@%s@@' % self.addr
        print 'all_comment_count=@@%d@@' % self.all_comment_count

        print 'default_comment_count=@@%d@@' % self.default_comment_count
        print 'group_comment_count=@@%d@@' % self.group_comment_count
        print 'short_comment_count=@@%d@@' % self.short_comment_count

        print 'five_star_count=@@%d@@' % self.five_star_count
        print 'four_star_count=@@%d@@' % self.four_star_count
        print 'three_star_count=@@%d@@' % self.three_star_count
        print 'two_star_count=@@%d@@' % self.two_star_count
        print 'one_star_count=@@%d@@' % self.one_star_count
        print 'has_parking_count=@@%d@@' % self.has_parking_count
        print 'has_pic_count=@@%d@@' % self.has_pic_count

        print 'download_datetime=@@%s@@' % self.download_datetime

        print 'first_show_comment_date=@@%s@@' % self.first_show_comment_date
        print 'last_show_comment_date=@@%s@@' % self.last_show_comment_date
        print 'show_comment_count=@@%d@@' % self.show_comment_count

        print 'search_conditions=@@%s@@' % self.search_conditions

        print 'note=@@%s@@' % escape_string(json.dumps(self.note))

    def create_table(self):
        """
            CREATE TABLE `tbl_shop` (
            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,

            `project` varchar(30) DEFAULT '' COMMENT '大种类',
            `category` varchar(30) DEFAULT '' COMMENT '小种类',
            `province` varchar(50) DEFAULT '' COMMENT '省份',
            `city` varchar(50) DEFAULT '' COMMENT '城市',
            `area` varchar(50) DEFAULT '' COMMENT '区域',
            `busi_area` varchar(30) DEFAULT '' COMMENT '商圈',

            `shope_name` varchar(50) DEFAULT '' COMMENT '店铺名称',
            `shop_url` varchar(150) DEFAULT '' COMMENT '大种类',
            `type` varchar(30) DEFAULT '' COMMENT '大种类',
            `comment_count` int DEFAULT 0 COMMENT '评论个数',
            `consume_average_str` varchar(30) DEFAULT '' COMMENT '人均消费(字符串)',
            `consume_average` float(10, 1) DEFAULT 0 COMMENT '人均消费',
            `taste_score` float(3, 1)  DEFAULT 0 COMMENT '口味得分',
            `env_score` float(3, 1) DEFAULT 0 COMMENT '环境得分',
            `serve_score` float(3, 1) DEFAULT 0 COMMENT '服务得分',
            `addr` varchar(100) DEFAULT '' COMMENT '地址',

            `all_comment_count` int DEFAULT 0 COMMENT '全部评论个数',
            `default_comment_count` int DEFAULT 0 COMMENT '默认评论个数',
            `group_comment_count` int DEFAULT 0 COMMENT '团购评论个数',
            `short_comment_count` int DEFAULT 0 COMMENT '签到短评个数',

            `five_star_count` int DEFAULT 0 COMMENT '5星个数',
            `four_star_count` int DEFAULT 0 COMMENT '4星个数',
            `three_star_count` int DEFAULT 0 COMMENT '3星个数',
            `two_star_count` int DEFAULT 0 COMMENT '2星个数',
            `one_star_count` int DEFAULT 0 COMMENT '1星个数',
            `has_parking_count` int DEFAULT 0 COMMENT '有停车信息个数',
            `has_pic_count` int DEFAULT 0 COMMENT '有图片个数',

            `first_show_comment_date` varchar(30) DEFAULT '' COMMENT '评论日期(最早可见)',
            `last_show_comment_date` varchar(30) DEFAULT '' COMMENT '评论日期(最晚可见)',
            `show_comment_count` int DEFAULT 0 COMMENT '可见评论个数(在最早可见和最晚可见时间段内的评论个数)',

            `tags` varchar(30) DEFAULT '' COMMENT 'tag列表',
            `search_conditions` varchar(100) DEFAULT '' COMMENT '搜索条件',

            `note` varchar(50) DEFAULT '' COMMENT '其他备注说明',

            `download_datetime` datetime DEFAULT '0000-00-00 00:00:00' COMMENT '数据下载日期',
            `insert_datetime` datetime DEFAULT CURRENT_TIMESTAMP,
            `update_datetime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,

            `data_status` int(3) DEFAULT NULL COMMENT '信息状态；-1:该信息一经被删除',
            PRIMARY KEY (`id`),
            UNIQUE KEY `shop_url` (`shop_url`)

        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='店铺搜索列表'
        """
        pass

    def is_downloaded(self, mysql_handle, logger):
        sql = 'select shop_url from tbl_shop where shop_url = "%s"' % self.shop_url
        res = mysql_handle.execute(sql_order=sql, logger=logger, auto_commit=False)

        if mysql_handle.cursor.fetchone() == None:
            return False
        else:
            return True

    def stand(self):
        pass

    def storn(self, mysql_handle, logger):
        sql = 'insert into tbl_shop set ' +\
            'project = "%s", ' % self.project +\
            'category = "%s", ' % self.category +\
            'province = "%s", ' % self.province +\
            'city = "%s", ' % self.city +\
            'area = "%s", ' % self.area +\
            'busi_area = "%s", ' % self.busi_area +\
            'shope_name = "%s", ' % self.shope_name +\
            'shop_url = "%s", ' % self.shop_url +\
            'type = "%s", ' % self.type +\
            'comment_count = %d, ' % self.comment_count +\
            'consume_average_str = "%s", ' % self.consume_average_str +\
            'consume_average = %.1f, ' % self.consume_average +\
            'taste_score = %.1f, ' % self.taste_score +\
            'env_score = %.1f, ' % self.env_score +\
            'serve_score = %.1f, ' % self.serve_score +\
            'addr = "%s", ' % self.addr +\
            'all_comment_count = %d, ' % self.all_comment_count +\
            'default_comment_count = %d, ' % self.default_comment_count +\
            'group_comment_count = %d, ' % self.group_comment_count +\
            'short_comment_count = %d, ' % self.short_comment_count +\
            'five_star_count = %d, ' % self.five_star_count +\
            'four_star_count = %d, ' % self.four_star_count +\
            'three_star_count = %d, ' % self.three_star_count +\
            'two_star_count = %d, ' % self.two_star_count +\
            'one_star_count = %d, ' % self.one_star_count +\
            'has_parking_count = %d, ' % self.has_parking_count +\
            'has_pic_count = %d, ' % self.has_pic_count +\
            'first_show_comment_date = "%s", ' % self.first_show_comment_date +\
            'last_show_comment_date = "%s", ' % self.last_show_comment_date +\
            'show_comment_count = %d, ' % self.show_comment_count +\
            'tags = "%s", ' % self.tags +\
            'search_conditions = "%s", ' % self.search_conditions +\
            'note = "%s", ' % self.note +\
            'download_datetime = "%s"' % self.download_datetime

        # logger.info(sql)

        res = mysql_handle.execute(sql_order=sql, logger=logger, auto_commit=True)
        if res < 0:
            logger.error(e)
            logger.error('sql=@@%s@@' % sql)
            # logger.info('raw_input')
            # raw_input("key...")


class shop_search_info_class(object):
    def __init__(self):
        self.name = ''
        self.addr = ''
        self.href = ''
        self.type = ''
        self.comment_count = ''
        self.consum_average = ''
        self.taste_score = ''
        self.env_score = ''
        self.serve_score = ''


class sta_ifno_class(object):
    """
    基于城市点评个数统计信息
    """

    def __init__(self):
        self.province = ''
        self.city = ''
        self.project = ''
        self.search_url = ''

        self.count = -1

    def create_table(self):
        """
            CREATE TABLE `tbl_sta` (
            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,

            `project` varchar(30) DEFAULT '' COMMENT '大种类',
            `province` varchar(50) DEFAULT '' COMMENT '省份',
            `city` varchar(50) DEFAULT '' COMMENT '城市',
            `search_url` varchar(200) DEFAULT '' COMMENT '搜索url',
            `count` int DEFAULT null COMMENT '个数',


            `note` varchar(50) DEFAULT '' COMMENT '其他备注说明',

            `download_datetime` datetime DEFAULT '00000-00-00 00:00:00' COMMENT '数据下载日期',
            `insert_datetime` datetime DEFAULT CURRENT_TIMESTAMP,
            `update_datetime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,

            `data_status` int(3) DEFAULT NULL COMMENT '信息状态；-1:该信息一经被删除',
            PRIMARY KEY (`id`),
            UNIQUE KEY `search_url` (`search_url`)

        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='基于城市,种类个数的统计'
        """

    def storn(self, mysql_handle, logger):

        sql = 'insert into tbl_sta set ' +\
            'project = "%s", ' % self.project + \
            'province = "%s", ' % self.province +\
            'city = "%s", ' % self.city + \
            'search_url = "%s" ' % self.search_url

        res = mysql_handle.execute(sql_order=sql, logger=logger, auto_commit=True)
        if res < 0:
            logger.error(e)
            logger.error('sql=@@%s@@' % sql)
            # logger.info('raw_input')
            # raw_input("key...")


class search_page_class(object):
    def __init__(self):
        self.project = ''
        self.province = ''
        self.city = ''
        self.search_url = ''
        self.status = ''

    def create_table(self):
        """
            CREATE TABLE `tbl_search_url` (
            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,

            `project` varchar(30) DEFAULT '' COMMENT '大种类',
            `category` varchar(30) DEFAULT '' COMMENT '小种类',
            `province` varchar(50) DEFAULT '' COMMENT '省份',
            `city` varchar(50) DEFAULT '' COMMENT '城市',
            `search_url` varchar(150) DEFAULT '' COMMENT '',

            `download_datetime` datetime DEFAULT '0000-00-00 00:00:00' COMMENT '数据下载日期',
            `insert_datetime` datetime DEFAULT CURRENT_TIMESTAMP,
            `update_datetime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,

            `data_status` int(3) DEFAULT NULL COMMENT '信息状态；-1:该信息一经被删除',
            PRIMARY KEY (`id`),
            UNIQUE KEY `search_url` (`search_url`)

        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='按城市, 种类搜索url表'
        """

    def storn(self, mysql_handle, logger):
        sql = 'insert into tbl_search_url set ' +\
            'project = "%s", ' % self.project +\
            'province = "%s", ' % self.province +\
            'city = "%s", ' % self.city +\
            'search_url = "%s", ' % self.search_url +\
            'download_datetime="%s"' % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        res = mysql_handle.execute(sql_order=sql, logger=logger, auto_commit=True)
        if res < 0:
            logger.error(e)
            logger.error('sql=@@%s@@' % sql)
            # logger.info('raw_input')
            # raw_input("key...")





