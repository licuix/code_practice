#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2017/11/9 下午2:37
# @Author  : licui
# @Site    : 
# @File    : test_data.py
# @Software: PyCharm

import pymysql
import requests

def sql_data(count):
    '搜出不一致的数据'
    customer_list=[]
    conn=pymysql.connect(host='127.0.0.1',port=3306,user='root',password='',charset='utf8')
    cursor=conn.cursor()
    cursor.execute('select * from customer.`customer_base` where `customer_id` not in (select `customer_id` '
                   'from crm.`crm_customer`)  limit %d' %count)
    values=cursor.fetchall()
    for data in values:
       customer_list.append(data[0])
    cursor.close()
    conn.close()
    return customer_list

def insert_data(count):
    '调用crm接口,插入数据'
    url="http://127.0.0.1:10160/base/update-customer"
    for customerId in sql_data(count):
       data={'customerId':customerId}
       print(customerId)
       r=requests.post(url,data=data)
       print(r.text)
       if r.text!='{"success":1}':
           break


insert_data(1)

