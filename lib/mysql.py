import logging
import pymysql

from config import Config

logging.basicConfig(filename='log.txt', level=logging.ERROR)


def get_conn():
    """
    :return: 连接，游标
    """
    
    conn = pymysql.connect(host=Config.DB_HOST,
                           port=Config.DB_PORT,
                           user=Config.DB_USERNAME,
                           password=Config.DB_PASSWORD,
                           db=Config.DB_DATABASE,
                           charset="utf8")
    cursor = conn.cursor()
    return conn, cursor



def close_conn(conn, cursor):
    cursor.close()
    conn.close()


def query(sql, *args):
    """
    :param sql:
    :param args:
    :return: 返回查询到的结果，((),(),)的形式
    """
    conn, cursor = get_conn()
    cursor.execute(sql, args)
    res = cursor.fetchall()
    close_conn(conn, cursor)
    return res
