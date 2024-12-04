import logging
import pymysql

from ...config import Config

logging.basicConfig(filename='log.txt', level=logging.ERROR)


def get_conn():
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
    conn, cursor = get_conn()
    cursor.execute(sql, *args)
    res = cursor.fetchall()
    close_conn(conn, cursor)
    return res


def execute(sql, *args):
    conn, cursor = get_conn()
    row = cursor.execute(sql, *args)
    conn.commit()
    close_conn(conn, cursor)
    return row

def query_dict(sql, *args):
    conn, cursor = get_conn()
    cursor.execute(sql, *args)
    result = cursor.fetchall()
    index_dict = get_index_dict(cursor)
    res = []
    for datai in result:
        resi = dict()
        for indexi in index_dict:
            resi[indexi] = datai[index_dict[indexi]]
        res.append(resi)
    close_conn(conn, cursor)
    return res


def query_key_dict(sql, *args, key):
    conn, cursor = get_conn()
    cursor.execute(sql, *args)
    result = cursor.fetchall()
    index_dict = get_index_dict(cursor)
    res = {}
    for datai in result:
        resi = dict()
        for indexi in index_dict:
            resi[indexi] = datai[index_dict[indexi]]
        res[resi[key]] = resi
    close_conn(conn, cursor)
    return res


def get_index_dict(cursor):
    index_dict = dict()
    index = 0
    for desc in cursor.description:
        index_dict[desc[0]] = index
        index = index + 1
    return index_dict

