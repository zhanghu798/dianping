#coding=utf8
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import urllib
import urllib2
import time
import requests
import urllib
import chardet
import gevent
from copy import deepcopy
from random import randint

G_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
G_BASE_DIR = os.path.dirname(G_CURRENT_DIR)
G_OUTPUT_DIR = os.path.join(G_BASE_DIR, 'output')

g_ua_list = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; TencentTraveler 4.0; .NET CLR 2.0.50727)",
    "User-Agent:Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11"
]

g_headder_dict = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": 'UTF-8',
    "Accept-Language": "zh-CN,zh;q=0.8,en;q=0.6",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:46.0) Gecko/20100101 Firefox/46.0",
    # "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    # "X-Anit-Forge-Token": "4a029523-e31b-4f43-a069-e76b5e6772fc",
    # "X-Anit-Forge-Code": "31387494",
    # "Referer": "https://www.lagou.com/gongsi/j116545.html",
    # "Content-Length": "73",
    'Connection': "keep-alive"
}


def get_ua():
    global g_ua_list
    return g_ua_list[randint(0, len(g_ua_list) - 1)]


def get_session(host='', is_use_proxy=False, ip='', port='', request_type='http', is_use_abuyun_proxy=False):
    global g_headder_dict

    if is_use_abuyun_proxy:
        # 使用阿布云代理服务器
        session = requests.session()

        ua = get_ua()
        header_dict = deepcopy(g_headder_dict)
        header_dict['User-Agent'] = ua

        proxy_meta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": "proxy.abuyun.com",
            "port": "9010",
            "user": "H152Y4C8V292Q58P",
            "pass": "E0ABE5935A5A6095",
        }


        
        proxies_dict = {
            "http": proxy_meta,
            "https": proxy_meta,
        }

        header_dict['Proxy-Switch-Ip'] = 'yes'

        if host != '':
            header_dict['HOST'] = host


        session.headers = header_dict
        session.proxies.update(proxies_dict)

        return session

    else:
        session = requests.session()

        ua = get_ua()
        header_dict = deepcopy(g_headder_dict)
        header_dict['User-Agent'] = ua

        # if host != '':
        #     header_dict['HOST'] = host

        session.headers = header_dict
        if is_use_proxy:
            proxies_dict = {
                request_type: "%s://%s:%s" % (request_type, ip, port)
            }
            session.proxies = proxies_dict


        return session


def download_use_session(session, url, logger, method='get', data={}, try_count=3, timeout=20):
    status_code = 0

    host = url.replace('http://www.', '').replace('https://', '').replace('http://www.', '').replace('http://', '').split('/')[0]
    session.headers['HOST'] = host
    for i in range(try_count):
        try:
            if method == 'get':
                requests_ret = session.get(url=url, timeout=timeout)
            elif method == 'post':
                requests_ret = session.post(url=url, timeout=timeout, data=data)
        except Exception as e:
            buf ="e=@@%s@@," % str(e) +\
                "url=@@%s@@, " % url +\
                "proxies=@@%s@@, " % str(session.proxies)

            logger.error(buf + '###' + str(e))
            gevent.sleep(15)
            continue

        request_url = urllib.unquote(str(requests_ret.url))
        chardet_detect_dict = chardet.detect(request_url)

        request_url = unicode(request_url, chardet_detect_dict['encoding'], 'ignore')

        status_code = requests_ret.status_code
        if status_code == 200 and request_url == request_url:
            return requests_ret, status_code
        else:
            status_code = requests_ret.status_code
            status_code = int(status_code)
            logger.warning('status_code:' + str(status_code) + ', url=' + url +\
                           ';session.header=' + str(session.headers) + '\n')
            if status_code == 404:
                return None, status_code

        gevent.sleep(10)

    logger.error(url + '\n')

    return None, status_code


if __name__ == '__main__':
    # test()
    pass
