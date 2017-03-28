#coding=utf8

import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from chardet import detect
from bs4 import BeautifulSoup
import re

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

from info_field import shop_info_class

def get_href_and_cat(soup, logger):
    item_list_list = []
    for a_match in soup.findAll('a'):
        href = a_match.get('href', '')

        if href == '':
            logger.error('href=')
        category = a_match.get_text().strip()
        item_list_list.append([href, category])

    return item_list_list

def display_list_list(logger, item_list_list):
    logger.info('')
    for item_list in item_list_list:
        logger.info('href=@@%s@@, item=@@%s@@' % (item_list[0], item_list[1]))


def parse_cat_condition_list(buf, logger):
    buf = unicode(buf, detect(buf)['encoding'], 'ignore')

    soup = BeautifulSoup(buf, "html.parser")

    nav_match_list = soup.select(".nav-category")

    cat_list_list = []

    for i in range(len(nav_match_list)):
        nav_match = nav_match_list[i]
        # with open(os.path.join(os.path.join(G_CURRENT_DIR, '_nav_%d.html' % i)), 'w') as f:
        #     f.write(str(nav_match))

        if nav_match.get_text().find(u'分类:') >= 0:
            # logger.info(nav_match.get_text())

            cat_list_list = get_href_and_cat(soup=nav_match, logger=logger)

    return cat_list_list


def parse_area_condition_list(buf, logger):
    buf = unicode(buf, detect(buf)['encoding'], 'ignore')

    soup = BeautifulSoup(buf, "html.parser")

    area_list_list = []

    bussi_nav_match_list = soup.select("#region-nav")
    if len(bussi_nav_match_list) > 0:
        area_list_list = get_href_and_cat(soup=bussi_nav_match_list[0], logger=logger)

    return area_list_list


def parse_search_condition_list(buf, logger):
    """
    根据搜索页内容解析搜索条件
    :param buf: 搜索页内容
    :param logger:
    :return: @cat_list_list,分类搜索条件列表[[url, 类别]...]. @area_list_list, 区域搜索页
    """
    buf = unicode(buf, detect(buf)['encoding'], 'ignore')

    soup = BeautifulSoup(buf, "html.parser")

    nav_match_list = soup.select(".nav-category")

    cat_list_list = []          # 分类搜索条件列表
    region_list_list = []         # 区域搜索条件列表

    for i in range(len(nav_match_list)):
        nav_match = nav_match_list[i]
        # with open(os.path.join(os.path.join(G_CURRENT_DIR, '_nav_%d.html' % i)), 'w') as f:
        #     f.write(str(nav_match))

        if nav_match.get_text().find(u'分类:')  >= 0:
            # logger.info(nav_match.get_text())

            cat_list_list = get_href_and_cat(soup=nav_match, logger=logger)


                # logger.info('href=@@%s@@, category=@@%s@@' % (href, category))

        elif nav_match.get_text().find(u'地点:')  >= 0:
            for a_match in nav_match.findAll('a'):
                if a_match.get_text() == u'行政区':
                    logger.info(str(a_match))

    bussi_nav_match_list = soup.select("#region-nav")
    if len(bussi_nav_match_list) > 0:
        region_list_list = get_href_and_cat(soup=bussi_nav_match_list[0], logger=logger)

        # display_list_list(logger=logger, item_list_list=item_list_list)

    return cat_list_list, region_list_list

# i = -1
def parse_busi_area_list(buf, logger):
    """
    解析按行政区域搜索结果页中的商圈
    :param buf:     按行政区域搜索的结果页
    :param logger:
    :return:
    """
    global i
    busi_area_list = []

    buf = unicode(buf, detect(buf)['encoding'], 'ignore')
    soup = BeautifulSoup(buf, 'html.parser')

    # i += 1
    # with open(os.path.join(G_OUT_DIR, 'busi_%d.html' % i), 'w') as f:
    #     f.write(str(soup))

    business_area_match_list = soup.select("#region-nav-sub")
    if len(business_area_match_list) != 0:
        busi_area_list = get_href_and_cat(soup=business_area_match_list[0], logger=logger)

    else:
        logger.info('not match!!!')

    return busi_area_list


def parse_shop_list(buf, logger):
    buf = unicode(buf, detect(buf)['encoding'], 'ignore')

    # with open(os.path.join(G_OUT_DIR, 'test.html'), 'w') as f:
    #     f.write(buf)

    with open(os.path.join(G_CURRENT_DIR, 'search.html'), 'w') as f:
        f.write(buf)
    #     logger.info('raw_input...')
    #     raw_input('key...')

    soup = BeautifulSoup(buf, 'html.parser')

    shop_info_handle_list = []
    for shop_html in re.findall(ur'(<li.*?</li>)', buf, flags=re.S):
        # logger.info('test')
        shop_info_handle = shop_info_class()

        with open('shop.html', 'w') as f:
            f.write(shop_html)

        match = re.search(ur'data-hippo-type="(.*)" title="([^"]+)" target=".*" href="([^"]+)"', shop_html)
        if match != None:
            data_type = match.group(1).strip()
            shope_name = match.group(2).strip()
            shope_href = match.group(3).strip()

            shop_info_handle.type = data_type
            shop_info_handle.shop_url = 'https://www.dianping.com/' + shope_href.strip('/')
            shop_info_handle.shope_name = shope_name


            # logger.info('data_type=@@%s@@, shope_name=@@%s@@, shope_href=@@%s@@' % (data_type, shope_name, shope_href))


        # logger.info('test')

        shop_url_match = re.search('href="(/shop/.*?)"', shop_html)
        if shop_url_match == None:
            continue

        copy_shop_html = re.sub(ur'<[/\w]*?>', '', shop_html)
        for replace_str in [u'<b>', u'</b>', u'</span>']:
            copy_shop_html =copy_shop_html.replace(replace_str, '')

        field_list_list = [
            ['comment_count', re.compile(ur'([\d]+)[\s]*条点评', flags=re.S)],
            ['consume_average_str', re.compile(ur'人均[\s]*([￥\d-]+)', flags=re.S)],
            ['taste_score', re.compile(ur'口味[\s]*([\d.-]+)', flags=re.S)],
            ['env_score', re.compile(ur'环境[\s]*([\d.-]+)', flags=re.S)],
            ['serve_score', re.compile(ur'服务[\s]*([\d.-]+)',  flags=re.S)]
        ]
        # logger.info('@@%s@@' % shop_html)
        info_dict = {}
        for field, pattern in field_list_list:
            match = pattern.search(copy_shop_html)
            if match != None:
                # logger.info('field=@@%s@@, value=@@%s@@' % (field, match.group(1)))

                info_dict[field] = match.group(1).strip()

        shop_info_handle.comment_count = int(info_dict.get('comment_count', -1))
        shop_info_handle.consume_average_str = info_dict.get('consume_average_str', '')
        try:
            shop_info_handle.taste_score = float(info_dict.get('taste_score', -1))
        except Exception as e:
            logger.info(info_dict.get('taste_score', -1))

        try:
            shop_info_handle.env_score = float(info_dict.get('env_score', -1))
        except Exception as e:
            logger.info(info_dict.get('env_score', -1))

        try:
            shop_info_handle.serve_score = float(info_dict.get('serve_score', -1))
        except Exception as e:
            logger.info(info_dict.get('serve_score', -1))

        tag_list = [tag for tag in re.findall(u'<span class="tag">(.*?)</span></a>', shop_html)]
        shop_info_handle.tags = ','.join(tag_list)

        # logger.info('tags=@@%s@@' % ','.join(tag_list))

        addr_match = re.search(ur'<span class="addr">(.*)</span>', shop_html)
        if addr_match != None:
            addr = addr_match.group(1).strip()
            shop_info_handle.addr = addr
            # logger.info(addr)


        match = re.search(ur'￥([\d.]+)', shop_info_handle.consume_average_str)
        if match != None:
            shop_info_handle.consume_average = float(match.group(1))


        shop_info_handle_list.append(shop_info_handle)

        # shop_info_handle.display()

    is_next = False
    if buf.find(u'title="下一页"') >= 0:
        is_next = True
    return shop_info_handle_list, is_next

def parse_shop_comment(buf, curr_page, logger):
    # buf = str(buf)
    # buf = unicode(buf, detect(buf)['encoding'], 'ignore')
    soup = BeautifulSoup(buf, 'html.parser')
    info_dict = {
        'all_comment_count': -1,
        'default_comment_count': -1,
        'group_comment_count': -1,
        'short_comment_count': -1,
        'search_cons': '',
        'five_star': -1,
        'four_star': -1,
        'three_star': -1,
        'two_star': -1,
        'one_star': -1,
        'search_conditions': '',
        'first_show_comment_date': '',
        'last_show_comment_date': '',
        'show_comment_count': -1,
        'has_parking_count': -1,
        'has_pic_count': -1
    }

    # 解析检索
    crumb_match_list = soup.select(".crumb")
    if crumb_match_list != []:
        search_con_list = []

        for li_match in crumb_match_list[0].findAll('li'):
            search_con = li_match.get_text().strip()
            search_con_list.append(search_con)

            info_dict['search_conditions'] = ','.join(search_con_list)

    comment_tab_match_list = soup.select('.comment-tab')
    if comment_tab_match_list != []:
        coment_stat_str = comment_tab_match_list[0].get_text()
        # logger.info('coment_stat_str=@@%s@@' % coment_stat_str)

        # 默认点评(293)团购点评(127)全部点评(293)签到短评(1)
        for field, key in [
            ['default_comment_count', ur'默认点评'],
            ['group_comment_count', ur'团购点评'],
            ['all_comment_count', ur'全部点评'],
            ['short_comment_count', ur'签到短评']
        ]:
            match = re.search(ur'%s\(([\d]+)\)' % key, coment_stat_str)
            if match != None:
                info_dict[field] = int(match.group(1))

            else:
                logger.error('match == None')

    comment_star_match_list = soup.select(".comment-star")
    if comment_star_match_list != []:
        satr_str = comment_star_match_list[0].get_text()

        for field, key in [
            ['five_star_count', u'5星'],
            ['four_star_count', u'4星'],
            ['three_star_count', u'3星'],
            ['two_star_count', u'2星'],
            ['one_star_count', u'1星']
        ]:
            match = re.search(ur'%s\(([\d]+)\)' % key, satr_str)
            if match != None:
                info_dict[field] = int(match.group(1))

            else:
                logger.error('match == None')

    filter_match_list = soup.select('.filter')
    if filter_match_list != []:
        filter_str = filter_match_list[0].get_text()
        # logger.info('action_str=@@%s@@' % filter_str)

        for field, key in [
            ['has_parking_count', u'有停车信息'],
            ['has_pic_count', u'有图片']
        ]:
            match = re.search(ur'%s\(([\d]+)\)' % key, filter_str)
            if match != None:
                info_dict[field] = int(match.group(1))

            else:
                logger.error('match == None')

    comment_list_match_list = soup.select('.comment-list')
    if comment_list_match_list != []:
        li_match_list = comment_list_match_list[0].select('.content')
        if li_match_list != []:
            show_comment_count = (curr_page - 1) * 20 + len(li_match_list)
            info_dict['show_comment_count'] = show_comment_count

            li_match = li_match_list[-1]
            time_match_list = li_match.select('.time')
            if time_match_list != []:
                info_dict['last_show_comment_date'] = '20' + time_match_list[0].get_text().split()[0].strip()

    return info_dict






