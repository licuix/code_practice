#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2017/11/10 下午4:43
# @Author  : licui
# @Site    : 
# @File    : ss.py
# @Software: PyCharm
import pymysql
import requests


import functools


def sql_connection(host=None, port=None, user=None, password=None, charset=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            with pymysql.connect(host=host, port=port, user=user, password=password, charset=charset) as cursor:
                    kw['cursor'] = cursor
                    result = func(*args, **kw)
                    return result
        return wrapper
    return decorator


@sql_connection(host='127.0.0.1', port=3306, user='root', password='', charset='utf8')
def query_data(count, **kw):
    cursor = kw['cursor']
    cursor.execute('select * from customer.`customer_base` where `customer_id` not in (select `customer_id` '
                   'from crm.`crm_customer`) limit %d' %count)
    return ({"customerId": data[0]} for data in cursor.fetchall())


def insert(url, count):
    return filter(lambda d: d.json()['success'] == '1', [requests.post(url, data=data) for data in query_data(count)])