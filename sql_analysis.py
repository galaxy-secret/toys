# coding: utf-8
# __author__ = panda

'''
分析sql语句
'''

import time
import subprocess
import re

from pymongo import MongoClient
import pymysql


mongo_config = {
    "host": "127.0.0.1",
    "port": 27017,
    "username": "root",
    "password": "root",
}
mongo = MongoClient(host=mongo_config['host'], port=mongo_config['port'],
                    username=mongo_config['username'], password=mongo_config['password'])

statistics_config = {
    "host": "127.0.0.1",
    "port": 3306,
    "username": "root",
    "password": "root",
    "db": "statistics",
}
statistics_mysql = pymysql.connect(host=statistics_config['host'],
                        port=statistics_config['port'],
                        user=statistics_config['username'],
                        password=statistics_config['password'],
                        db=statistics_config['db'],
                        charset='utf8mb4',
                        cursorclass=pymysql.cursors.DictCursor
                        )

biz_config = {
    "host": "127.0.0.1",
    "port": 3306,
    "username": "root",
    "password": "root",
    "db": "business",
}
biz_mysql = pymysql.connect(host=biz_config['host'],
                            port=biz_config['port'],
                            user=biz_config['username'],
                            password=biz_config['password'],
                            db=biz_config['db'],
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor
                            )

user_config = {
    "host": "127.0.0.1",
    "port": 3306,
    "username": "root",
    "password": "root",
    "db": "users",
}
user_mysql = pymysql.connect(host=user_config['host'],
                             port=user_config['port'],
                             user=user_config['username'],
                             password=user_config['password'],
                             db=user_config['db'],
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor
                             )


def getSql(collection='statistics'):
    '''从mongo中拿sql'''
    sql_statistics = mongo['sql'][collection]
    for sql in sql_statistics.find():
        yield sql['0']


def preSql():
    '''处理语句 主要是滤重 只要查询语句'''
    temp_sql = set()
    for collection in ['statistics', 'biz', 'user']:
        for sql in getSql(collection=collection):
            if sql.startswith('SELECT '):
                temp_sql.add(sql)
    return temp_sql

def getTableName(connection):
    '''获取相应的库的所有表名称'''
    cursor = connection.cursor()
    cursor.execute("show tables")
    tables = cursor.fetchall()
    table_names = []
    for temp in tables:
        table_names = table_names + list(temp.values())
    return table_names

def analysis_sql(sql, connection):
    '''分析语句, 并将结果保存到 mongodb 中'''
    cursor = connection.cursor()
    cursor.execute("explain " + sql)
    res = cursor.fetchone()
    res['raw_sql'] = sql
    mongo['sql']['analysis_statistics'].insert_one(res)

if __name__ == '__main__':
    '''启动方法'''
    '''获取库中的所有表名'''
    statistics_table_names = getTableName(statistics_mysql)
    biz_table_names = getTableName(biz_mysql)
    user_table_names = getTableName(user_mysql)
    '''循环遍历出所有的 sql 语句'''
    for sql in preSql():
        '''找出语句中的表名'''
        table_name = re.search(r'FROM (\w*) t', sql, re.I).group(1)
        '''处理相应的库, explain所有的 query 语句'''
        if table_name in statistics_table_names:
            analysis_sql(sql, statistics_mysql)
        elif table_name in biz_table_names:
            analysis_sql(sql, biz_mysql)
        elif table_name in user_table_names:
            analysis_sql(sql, user_mysql)


