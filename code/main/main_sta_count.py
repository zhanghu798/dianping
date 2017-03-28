#coding=utf8

import os


import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from datetime import datetime
import pandas as pd
import gevent
from gevent import monkey; monkey.patch_all()

from random import randint
import re


G_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
G_CODE_DIR = os.path.dirname(G_CURRENT_DIR)
G_ROOT_DIR = os.path.dirname(G_CODE_DIR)


G_LOG_DIR = os.path.join(G_ROOT_DIR, 'log')
G_OUT_DIR = os.path.join(G_ROOT_DIR, 'out')

# print "=====G_LOG_DIR=@@%s@@" % G_LOG_DIR
for dir in [G_LOG_DIR, G_OUT_DIR]:
    if not os.path.exists(dir):
        os.mkdir(dir)
#         print 'not dir is exists =@@%s@@' % dir
#
#     else:
#         print 'dir is exists =@@%s@@' % dir
#
# raw_input('key...')

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
    from ..parser.info_field import (search_condition_class, sta_ifno_class)


except Exception as e:
    from logger import get_logger
    from my_mysql import mysql_class
    from spider import get_session
    from spider import download_use_session
    from info_field import (search_condition_class, sta_ifno_class)


from bs4 import BeautifulSoup
from chardet import detect

def get_city_list():
    logger = get_logger('get_sta_base_city.log')
    session = get_session(is_use_abuyun_proxy=False, is_use_proxy=False)
    # import requests
    # session = requests.session()
    with open('./city_list.html', 'r') as f:
        buf = f.read()
        buf = unicode(buf, detect(buf)['encoding'], 'ignore')

    soup = BeautifulSoup(buf, 'html.parser')

    search_info_list_list = []
    for terms_match in soup.select('.terms'):
        for a_match in terms_match.findAll('a'):
            href = a_match['href'].strip()
            city = a_match.get_text().strip()

            search_info_list_list.append(['', city, href])




    for terms_open_match in soup.select('.terms-open'):
        dt_match = terms_open_match.find('dt')
        if dt_match != None:
            print '*'*100
            province = dt_match.get_text().strip()
            print 'province=@@%s@@' % province
        else:
            print "error"
            raw_input('key...')
        for a_match in terms_open_match.findAll('a'):
            href = a_match['href']

            city = a_match.get_text().strip()

            search_info_list_list.append([province, city, href])

    mysql_handle = mysql_class(db='dianping')
    for province, city, href in search_info_list_list:
        print 'city=@@%s@@, href=@@%s@@' % (city, href)

        for project_info_list in [
            ['food', u'美食'],
            ['movie', u'电影'],
            ['life', u'休闲娱乐'],
            ['hotel', u'酒店'],
            ['beauty', u'丽人', 0],
            ['sports', u'运动健身'],
            ['KTV', u'K歌'],
            ['view', u'周边游'],

            ['baby', u'亲子', 0],
            ['wedding', u'结婚', 0],

            ['shopping', u'购物'],
            ['pet', u'宠物'],
            ['other', u'生活服务'],

            ['education', u'学习培训', 0],
            ['car', u'爱车'],
            ['medical', u'医疗健康'],
            ['home', u'家装', 0],
            ['hall', u'宴会', 0]
        ]:
            if city == u'更多':
                continue
            if len(project_info_list) > 2:
                continue

            logger.info('*'*100)
            search_url = href.strip('/') + '/' + project_info_list[0]
            # search_url = 'http://www.dianping.com/beijing' + '/' + project_info_list[0]
            print 'search_url=@@%s@@' % search_url

            sta_info_handle = sta_ifno_class()
            sta_info_handle.province = province
            sta_info_handle.city = city
            sta_info_handle.project = project_info_list[1]
            sta_info_handle.search_url = search_url

            sta_info_handle.storn(mysql_handle=mysql_handle, logger=logger)

            continue


    exit(0)


def get_sta_result():
    logger = get_logger('get_sta_result.log')
    session = get_session(is_use_abuyun_proxy=False, is_use_proxy=False)
    mysql_handle = mysql_class(db='dianping')

    sql = 'select id, project,province,city,search_url from tbl_sta where count is null'
    sql = 'select id, project,province,city,search_url from tbl_sta where id > (select max(id) from tbl_sta  where count is  not  null)'

    search_df = pd.read_sql(sql=sql, con=mysql_handle.conn)

    for i in range(search_df.shape[0]):
        data_series = search_df.iloc[i, :]
        record_id = data_series.id
        project = data_series.project
        city = data_series.city
        search_url = data_series.search_url

        logger.info('url=@@%s@@' % search_url)

        res = session.get(search_url)

        if res.status_code != 200:
            logger.error('status code=%d' % res.status_code)
            continue

        buf = res.content
        # with open('text.html', 'w') as f:
        #     f.write(buf)
        # buf = unicode(buf, detect(buf)['encoding'], 'ignore')
        soup = BeautifulSoup(buf, 'html.parser')

        sign_find_count = False
        count = -1
        match = re.search(r'<em>([\d]+)家</em>', buf)
        if match != None:
            count = int(match.group(1))
        else:
            # logger.info('not find..')
            for pattern in ['.hd', '.block-title', '.detail', ]:
                block_title_match_list = soup.select(pattern)
                if block_title_match_list != []:
                    count_str = unicode(block_title_match_list[0])
                    # print 'count_str=@@%s@@' % count_str
                    # print 'count_str=@@%r@@' % count_str
                    match = re.search(ur'([\d]+)家', count_str)
                    if match != None:
                        count = int(match.group(1))
                        # logger.info('count=%d' % int(count))
                        break
                    else:
                        match = re.search(ur'([\d]+)', count_str)
                        if match != None:
                            count = int(match.group(1))
                            # logger.info('count=%d' % int(count))
                            break
                        # logger.error('match == None')
                else:
                    # logger.error("not fnd black title")
                    # logger.error('key...')
                    pass
        # logger.info('count=%d' % count)
        # raw_input('key...')

        sql = 'update tbl_sta set count=%d where id = %d' % (count, record_id)
        res = mysql_handle.execute(sql_order=sql, logger=logger, auto_commit=True)
        if res < 0:
            logger.error('res=%d' % res)
            logger.info('key...')
            raw_input('key...')


if __name__ == '__main__':
    # get_city_list()
    get_sta_result()
