import pymysql

from config import Config


class MySQL(object):
    _instance = None

    @staticmethod
    def get_instance():
        
        
        
        
        
        
        
        
        MySQL._instance = MySQL()
        return MySQL._instance

    def __init__(self):
        self.conn = pymysql.connect(host=Config.DB_HOST,
                                    port=Config.DB_PORT,
                                    user=Config.DB_USERNAME,
                                    password=Config.DB_PASSWORD,
                                    db=Config.DB_DATABASE,
                                    charset="utf8")
        self.cursor = self.conn.cursor()


def query(sql, *args):
    """
    :param sql:
    :param args:
    :return: 返回查询到的结果，((),(),)的形式
    """
    obj = MySQL.get_instance()
    obj.cursor.execute(sql, args)
    res = obj.cursor.fetchall()
    obj.conn.close()
    return res


def execute(sql, *args):
    obj = MySQL.get_instance()
    row = obj.cursor.execute(sql, *args)
    obj.conn.commit()
    obj.conn.close()
    return row


def query_dict(sql, *args):
    """
    :param sql:
    :param args:
    :return: 返回查询到的结果，dict的形式
    """
    obj = MySQL.get_instance()
    obj.cursor.execute(sql, *args)
    result = obj.cursor.fetchall()
    index_dict = get_index_dict(obj.cursor)
    res = []
    for datai in result:
        resi = dict()
        for indexi in index_dict:
            resi[indexi] = datai[index_dict[indexi]]
        res.append(resi)
    obj.conn.close()
    return res


def query_key_dict(sql, *args, key):
    """
    :param sql:
    :param args:
    :return: 返回查询到的结果，key:dict的形式
    """
    obj = MySQL.get_instance()
    obj.cursor.execute(sql, *args)
    result = obj.cursor.fetchall()
    index_dict = get_index_dict(obj.cursor)
    res = {}
    for datai in result:
        resi = dict()
        for indexi in index_dict:
            resi[indexi] = datai[index_dict[indexi]]
        res[resi[key]] = resi
    obj.conn.close()
    return res


def get_index_dict(cursor):
    """
    获取数据库对应表中的字段名
    """
    index_dict = dict()
    index = 0
    for desc in cursor.description:
        index_dict[desc[0]] = index
        index = index + 1
    return index_dict
