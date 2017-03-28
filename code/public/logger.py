#coding=utf8

import sys
import requests
import hashlib
reload(sys)
sys.setdefaultencoding("utf-8")

import logging


def get_logger(log_file, log_name='', display=True):
    if log_name != '':
        formatter = logging.Formatter('%(name)s==>%(levelname)s:%(asctime)-15s; %(filename)s %(funcName)s %(lineno)d; %(message)s')
    else:
        formatter = logging.Formatter('%(levelname)s:%(asctime)-15s; %(filename)s %(funcName)s %(lineno)d; %(message)s')

    # logging.basicConfig(format=formatter, filename=log_file)
    # logger = logging.getLogger(log_name)
    # logger.setLevel('DEBUG')

    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if display:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)


    # logger.setLevel('NOTSET')
    # print "id(logger)= %d" % id(logger)
    return logger


def test():
    logger = get_logger('./test.log')
    logger.debug("this is a debug!!!")
    logger.info("this is a info!!!")
    logger.warning("this is a warning!!!")
    logger.error("this is a error")

if __name__ == '__main__':
    test()
