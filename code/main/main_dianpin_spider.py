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
    from ..parser.info_field import (
        search_condition_class,
        sta_ifno_class)



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
    from info_field import search_condition_class


def test_spider():
    logger = get_logger(G_LOG_DIR, 'test.log')
    logger.info('strart...')



    session = get_session(host='www.dianping.com', is_use_abuyun_proxy=True)

    # logger.info(session.headers)

    url = 'https://www.dianping.com'

    res, status_code = download_use_session(session=session, url=url, try_count=6, logger=logger)

    url = 'https://www.dianping.com/search/category/2/10/g110'

    logger.info('url=@@%s@@' % url)

    res, status_code = download_use_session(session=session, url=url, try_count=6, logger=logger)

    if res == None:
        logger.error("res=None")
        return None


    parse_shop_list(buf=res.content, logger=logger)

    logger.info('end')


def get_search_condition(search_url, city, project, logger, province=''):
    logger.info('strart...')


    mysql_handle = mysql_class(db='dianping')
    session = get_session(is_use_abuyun_proxy=True, is_use_proxy=False)

    url = 'https://www.dianping.com'
    res, status_code = download_use_session(session=session, url=url,try_count=6, logger=logger)
    logger.info(res)

    res, status_code = download_use_session(session=session, url=search_url,try_count=6, logger=logger)

    if res == None:
        logger.error("res=None")
        return None

    cat_list_list = parse_cat_condition_list(buf=res.content, logger=logger)

    for cat_href, cat in cat_list_list:

        # logger.info('\n\n' + ('*'* 100))
        cat_href = 'https://www.dianping.com' + cat_href

        # if cat == u'不限':
        #     continue

        # logger.info('@@%s@@%s@@' % (cat_href, cat))

        res, status_code = download_use_session(session=session, url=cat_href, try_count=6, logger=logger)

        if res == None:
            logger.error("res=None")
            continue

        # logger.info('')

        area_list_list = parse_area_condition_list(buf=res.content, logger=logger)

        for area_href, area in area_list_list:
            area_href = 'https://www.dianping.com' + area_href

            # if area == u'不限':
            #     continue


            # logger.info('\n\n' + ('#' * 100))
            # logger.info('@@%s@@%s@@' % (area_href, area))



            res, status_code = download_use_session(session=session, url=area_href,try_count=6, logger=logger)

            if res == None:
                logger.error("res=None")
                continue

            busi_area_list_list = parse_busi_area_list(buf=res.content, logger=logger)
            for busi_area_href, busi_area in busi_area_list_list:
                busi_area_href = 'https://www.dianping.com' + busi_area_href
                # logger.info('@@%s@@%s@@' % (busi_area_href, busi_area))

                # logger.info('*'*100)
                # logger.info('city=@@%s@@' % city)
                # logger.info('project=@@%s@@' % project)
                # logger.info('cat=@@%s@@' % cat)
                # logger.info('area=@@%s@@' % area)
                # logger.info('busi_area=@@%s@@' % busi_area)
                # logger.info('url=@@%s@@' % busi_area_href)

                search_condition_info_handle = search_condition_class()

                search_condition_info_handle.province = province
                search_condition_info_handle.city = city
                search_condition_info_handle.area = area
                search_condition_info_handle.busi_area = busi_area

                search_condition_info_handle.project = project
                search_condition_info_handle.category = cat
                search_condition_info_handle.download_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                search_condition_info_handle.search_url = busi_area_href

                # search_condition_info_handle.display()

                # logger.info('key...')
                # raw_input('key...')

                res = search_condition_info_handle.storn_search_conditon(
                    mysql_handle=mysql_handle,
                    logger=logger
                )

                logger.info('res=%d' % res)


                # #
    # with open(os.path.join(G_OUT_DIR, 'search.hmtl'), 'w') as f:
    #     f.write(res.content)
    # cat_list_list, region_list_list = parser_search_condition_list(buf=res.content, logger=logger)

    #
    # logger.info('*'*100 + u'类别')
    # for cat_list in cat_list_list:
    #     logger.info('@@%s@@%s@@' % (cat_list[0], cat_list[1]))
    #
    #
    # logger.info('*'*100 + u'类别')
    #
    # for cat_list in region_list_list:
    #     logger.info('@@%s@@%s@@' % (cat_list[0], cat_list[1]))
    #
    # for href, area in region_list_list:
    #     href = 'https://www.dianping.com' + href
    #
    #     logger.info('area=@@%s@@, href=@@%s@@' % (area, href))
    #
    #     res, status_code = download_use_session(session=session, url=href, logger=logger)
    #
    #     if res != None:
    #         with open(os.path.join(G_OUT_DIR, '%s.html' % area), 'w') as f:
    #             f.write(res.content)
    #
    #
    #     busi_list = parser_busi_area_list(buf=res.content, logger=logger)
    #
    #     for href, busi_area in busi_list:
    #         href = 'https://www.dianping.com' + href
    #
    #         logger.info('busi_area=@@%s@@, href=@@%s@@' % (busi_area, href))
    #
    #
    #
    # logger.info('end')


def get_detail_page(session, detail_page_url,  logger, shop_info_handle):
    res, status = session.get(detail_page_url)
    if res != None:
        pass


def get_one_search_page(session, search_url, mysql_handle, logger, project, category, province, area, busi_area, city):
    res, status_code = download_use_session(session=session, url=search_url,try_count=6, logger=logger)
    if res == None:
        logger.error('res == None')
        return None, 0
    content = res.content
    shop_info_handle_list, is_next = parse_shop_list(content, logger)

    gevent.sleep(randint(0, 3))

    # print '\n\n'
    # logger.info('')
    # print '*'*100

    # logger.info('len(shop_info_handle_list)=%d' % len(shop_info_handle_list))

    shop_count = 0
    for shop_info_handle in shop_info_handle_list:
        shop_info_handle.project = project
        shop_info_handle.category = category
        shop_info_handle.province = province
        shop_info_handle.city = city
        shop_info_handle.area = area
        shop_info_handle.busi_area = busi_area


        is_downloaded = shop_info_handle.is_downloaded(mysql_handle, logger)
        if is_downloaded == True:
            shop_count += 1
            continue

        comment_count = shop_info_handle.comment_count
        max_page = (comment_count - 1) / 20 + 1

        # detail_page = 'https://www.dianping.com/shop/67027668/review_all?pageno=28'
        last_comment_page_url = shop_info_handle.shop_url + '/review_more?pageno=%d' % max_page

        logger.info(last_comment_page_url)

        res, status = download_use_session(session=session, url=last_comment_page_url,try_count=6, logger=logger)
        if res != None:
            info_dict = parse_shop_comment(buf=res.content, logger=logger, curr_page=max_page)
            if info_dict == None:
                logger.error('parse last comment page is failue')
                gevent.sleep(randint(0, 3))

                continue

            for key in info_dict:
                value = info_dict[key]
                if isinstance(value, (str, unicode)):
                    value = '"%s"' % value
                try:
                    exec ('shop_info_handle.%s=%s' % (key, value))
                except Exception as e:
                    logger.error(e)
                    continue

        gevent.sleep(randint(0, 3))

        # print '\n'
        # print '#'*100
        # shop_info_handle.display()


        shop_count += 1
        shop_info_handle.download_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        shop_info_handle.storn(mysql_handle=mysql_handle, logger=logger)

        # logger.info('raw')
        # raw_input('key...')

    return is_next, shop_count


def main_get_shop_info(process_count, unlimited):
    mysql_handle = mysql_class(host='101.201.113.127', db='dianping')

    if unlimited:
        sql = """
            select
                count(*)
            from
                tbl_search_condition
        """
    else:
        sql = """
            select
                count(*)
            from
                tbl_search_condition
            where
                busi_area != '不限'
            and
                downloaded_shop_count <= 0
        """

    df = pd.read_sql(sql=sql, con=mysql_handle.conn)

    all_count = df.iloc[0, :][0]
    per_task_count = all_count/process_count + 1

    print 'all_count=%d, process_count=%d, per_task_count=%d' % (all_count, process_count, per_task_count)


    event_spawn_list = []

    for process_num in range(process_count):
        event_spawn_list.append(
            gevent.spawn(get_shop_info, process_num, per_task_count, unlimited)
        )


    gevent.joinall(event_spawn_list)





def get_shop_info(spider_num, task_count=-1, unlimited=None):
    logger = get_logger(
        log_file=os.path.join(G_LOG_DIR, 'search_page_%d.log' % spider_num),
        log_name='event_%d' % spider_num)
    gevent.sleep(spider_num*15-spider_num)

    mysql_handle = mysql_class(db='dianping')

    if unlimited:
        sql = """
            select
                id,
                project,
                category,
                province,
                city,
                area,
                busi_area,
                search_url
            from
                tbl_search_condition
        """

    else:
        sql = """
            select
                id,
                project,
                category,
                province,
                city,
                area,
                busi_area,
                search_url
            from
                tbl_search_condition
            where
                busi_area != '不限'
            and
                downloaded_shop_count <= 0
        """

    if task_count != -1:
        sql += 'limit %d, %d' % (spider_num*task_count, task_count)


    search_condition_df = pd.read_sql(sql=sql, con=mysql_handle.conn)

    for i in range(search_condition_df.shape[0]):
        data_series = search_condition_df.iloc[i, :]
        search_record_id = data_series.id
        project = data_series.project
        category = data_series.category
        province = data_series.province
        city = data_series.city
        area = data_series.area
        busi_area = data_series.busi_area
        search_url = data_series.search_url


        logger.info('*'*100)
        logger.info('search_id=%d, search_url=@@%s@@' % (search_record_id, search_url))

        start_page = 0
        all_shop_count = 0
        while 1:
            start_page += 1
            if start_page == 1:
                url = search_url + 'o10'
            else:
                url = search_url + 'o10p%d' % start_page

            session = get_session(is_use_abuyun_proxy=True)

            logger.info('url=@@%s@@' % url)

            is_next, shop_count = get_one_search_page(session=session,
                                search_url=url,
                                logger=logger,
                                mysql_handle=mysql_handle,
                                project=project,
                                category=category,
                                province=province,
                                area=area,
                                busi_area=busi_area,
                                city=city)


            # logger.info(is_next)

            # logger.info('raw_input')
            # raw_input('key...'
            #           '')

            all_shop_count += shop_count

            if not is_next:
                break


        gevent.sleep(0)



        # 设置该搜索条件已经搜索到的店铺个数

        logger.info('downloaded_shop_count=%d' % all_shop_count)

        sql = 'update tbl_search_condition set downloaded_shop_count = %d where id = %d' % \
              (all_shop_count, search_record_id)

        res = mysql_handle.execute(sql_order=sql, logger=logger, auto_commit=True)
        if res < 0:
            logger.error('res=%d' % res)


if __name__ == '__main__':
    argc_list = sys.argv[1:]

    if len(argc_list) < 1:
        print """
            ***********************************
            not argc, example
            'search': func, search condition
            'detail': 'func, shop info
                'yes': argc, is unlimited?

        """
        exit(0)
    else:
        print ','.join(argc_list)

    if argc_list[0] == 'search':
        logger = get_logger(os.path.join(G_LOG_DIR, 'get_search.log'))

        print 'to get search condition...'
        # city = u'北京'
        # search_url = 'http://www.dianping.com/search/category/2/10'
        # project = u'美食'
        #
        # city = u'上海'
        # search_url = 'http://www.dianping.com/search/category/1/10'
        #
        # city = u'杭州'
        # search_url = 'http://www.dianping.com/search/category/3/10'

        # for city, search_url in [
        #     # [u'南京','http://www.dianping.com/search/category/5/10'],
        #     # [u'苏州', 'http://www.dianping.com/search/category/6/10']
        #     # [u'深圳', 'http://www.dianping.com/search/category/7/10'],
        #     # [u'成都', 'http://www.dianping.com/search/category/8/10'],
        #     # [u'重庆', 'http://www.dianping.com/search/category/9/10'],
        #     # [u'天津', 'http://www.dianping.com/search/category/10/10'],
        #     # [u'宁波', 'http://www.dianping.com/search/category/11/10'],
        #     # [u'扬州', 'http://www.dianping.com/search/category/12/10'],
        #     [u'福州', 'http://www.dianping.com/search/category/14/10'],
        #     [u'厦门', 'http://www.dianping.com/search/category/15/10'],
        #     [u'武汉', 'http://www.dianping.com/search/category/16/10'],
        #     [u'西安', 'http://www.dianping.com/search/category/17/10'],
        #     [u'沈阳', 'http://www.dianping.com/search/category/18/10'],
        #     [u'大连', 'http://www.dianping.com/search/category/19/10'],
        #     [u'大连', 'http://www.dianping.com/search/category/19/10'],
        #     [u'青岛', 'http://www.dianping.com/search/category/21/10'],
        #     [u'济南', 'http://www.dianping.com/search/category/22/10'],
        #     [u'海口', 'http://www.dianping.com/search/category/23/10'],
        #     [u'石家庄', 'http://www.dianping.com/search/category/24/10'],
        #     [u'唐山', 'http://www.dianping.com/search/category/25/10'],
        #     [u'秦皇岛', 'http://www.dianping.com/search/category/26/10'],
        #     [u'邯郸', 'http://www.dianping.com/search/category/27/10'],
        #     [u'邢台', 'http://www.dianping.com/search/category/28/10'],
        #     [u'保定', 'http://www.dianping.com/search/category/29/10'],
        #     [u'张家口', 'http://www.dianping.com/search/category/30/10'],
        #     [u'承德', 'http://www.dianping.com/search/category/31/10'],
        #     [u'沧州', 'http://www.dianping.com/search/category/32/10'],
        #     [u'廊坊', 'http://www.dianping.com/search/category/33/10'],
        #     [u'衡水', 'http://www.dianping.com/search/category/34/10'],
        #     [u'太原', 'http://www.dianping.com/search/category/35/10'],
        #     [u'大同', 'http://www.dianping.com/search/category/36/10'],
        #     [u'阳泉', 'http://www.dianping.com/search/category/37/10']
        #
        #
        #
        # ]:
        #     # get_search_condition(search_url=search_url, city=city, project=project)
        #     pass


        mysql_handle = mysql_class(host='101.201.113.127', db='dianping')

        sql = 'select project, province, city, search_url from tbl_search_url'
        df = pd.read_sql(sql=sql, con=mysql_handle.conn)

        for i in range(df.shape[0]):
            data_series = df.iloc[i, :]
            project = data_series.project
            province = data_series.province
            city = data_series.city
            search_url = data_series.search_url.strip('//') + '/10'
            logger.info('*'*100)
            logger.info('city=%s, url=@@%s@@' % (city, search_url))

            get_search_condition(search_url=search_url, city=city, project=project, province=province, logger=logger)

        print 'search end!!!'

    elif argc_list[0] == 'detail':
        print 'to get detaiil...'
        unlimited = False
        if (len(argc_list) > 1) and (argc_list[1] == 'yes'):
            unlimited = True

        main_get_shop_info(process_count=20, unlimited=unlimited)
        print 'search end!!!'
