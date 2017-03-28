#coding=utf8

import os


import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from datetime import datetime
import pandas as pd
import gevent
from gevent import monkey; monkey.patch_all()
from chardet import detect
from bs4 import BeautifulSoup
import re

from random import randint


G_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
G_CODE_DIR = os.path.dirname(G_CURRENT_DIR)
G_ROOT_DIR = os.path.dirname(G_CODE_DIR)


G_LOG_DIR = os.path.join(G_ROOT_DIR, 'log')
G_OUT_DIR = os.path.join(G_ROOT_DIR, 'out')

# print "=====G_LOG_DIR=@@%s@@" % G_LOG_DIR
for dir in [G_LOG_DIR, G_OUT_DIR]:
    if not os.path.exists(dir):
        os.mkdir(dir)

# sys.path.append(G_ROOT_DIR)
sys.path.append(os.path.join(G_CODE_DIR, 'public'))
sys.path.append(os.path.join(G_CODE_DIR, 'parser'))


try:
    from ..public.logger import get_logger
    from ..public.my_mysql import mysql_class
    from ..public.spider import get_session
    from ..public.spider import download_use_session
    from ..parser.parser_list import (
        parse_shop_list,
        parse_search_condition_list,
        parse_busi_area_list,
        parse_cat_condition_list,
        parse_area_condition_list,
        parse_shop_comment,
    )
    from ..parser.info_field import (
        search_condition_class,
        sta_ifno_class,
        search_page_class
    )



except Exception as e:
    from logger import get_logger
    from my_mysql import mysql_class
    from spider import get_session
    from spider import download_use_session
    from parser_list import (
        parse_shop_list,
        parse_search_condition_list,
        parse_busi_area_list,
        parse_cat_condition_list,
        parse_area_condition_list,
        parse_shop_comment
    )
    from info_field import (
        search_condition_class,
        search_page_class
    )



def parser_search_page(content, logger):
    content = unicode(content, detect(content)['encoding'], 'ignore')
    soup = BeautifulSoup(content, 'html.parser')

    city_match_list = soup.select(".city")
    if city_match_list != []:
        city = city_match_list[0].get_text().strip()
        logger.info('title=@@%s@@' % city)
    else:
        logger.error('not find city')
        return {}

    search_url_list = re.findall(ur'http://www.dianping.com/search/category/[\d]+/', content)

    search_url_list = list(set(search_url_list))
    
    if len(search_url_list) != 1:
        logger.error('len(search_url_list)=%d' % len(search_url_list))
        
    
        return {}
    
    return {'city': city, 'search_url': search_url_list[0].strip('/')}






def get_search_url():
    logger = get_logger(os.path.join(G_LOG_DIR, 'get_search_url.log'))
    mysql_hanle = mysql_class(host='101.201.114.127', db='dianping')

    project = u'美食'
    sql = "select id, province, city, search_url  from tbl_sta  where project = '%s' and id > 28717 ;" % project

    search_df = pd.read_sql(sql=sql, con=mysql_hanle.conn)

    session = get_session(is_use_abuyun_proxy=True)

    for i in range(search_df.shape[0]):
        search_url_handle = search_page_class()
        data_series = search_df.iloc[i, :]

        record_id = data_series.id
        if record_id % 100 == 0:
            logger.info('*************************NO.%d' % record_id)
        city_url = data_series.search_url
        search_url_handle.project = project
        search_url_handle.city = data_series.city
        search_url_handle.province = data_series.province

        res, status = download_use_session(session=session, url=city_url, logger=logger)

        if res == None:
            logger.error('res==None')
            # logger.info('key...')
            # raw_input('key...')

            continue


        # with open('search_url.html', 'w') as f:
        #     f.write(res.content)

        parser_dict = parser_search_page(content=res.content, logger=logger)
        if parser_dict == {}:
            logger.warning('city_url=@@%s@@' % city_url)
            continue
        if parser_dict['city'] != search_url_handle.city:
            logger.info('province=@@%s@@, city_url=@@%s@@' % (search_url_handle.province, city_url))
            logger.info('search_url==@@%s@@' % parser_dict['search_url'])
            logger.error('parser_city=@@%s@@, search_city=@@%s@@' % (parser_dict['city'], search_url_handle.city))
            logger.info('key...')
            # raw_input('key...')
            continue


        search_url_handle.search_url = parser_dict['search_url']

        search_url_handle.storn(mysql_handle=mysql_hanle, logger=logger)


        # logger.info('key...')
        # raw_input('key...')



if __name__ == '__main__':
    get_search_url()





