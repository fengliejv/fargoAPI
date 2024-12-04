import pymysql

from config.common import Config


class MySQL(object):
    _instance = None

    @staticmethod
    def get_instance():
        MySQL._instance = MySQL()
        return MySQL._instance

    def __init__(self):
        try:
            self.conn = pymysql.connect(host=Config.DB_HOST,
                                        port=Config.DB_PORT,
                                        user=Config.DB_USERNAME,
                                        password=Config.DB_PASSWORD,
                                        db=Config.DB_DATABASE,
                                        charset="utf8mb4")
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"{Config.DB_HOST}{Config.DB_PORT}{Config.DB_USERNAME}{Config.DB_PASSWORD}{Config.DB_DATABASE}{e}")


def query(sql, *args):
    obj = MySQL.get_instance()
    obj.cursor.execute(sql, *args)
    res = obj.cursor.fetchall()
    obj.conn.close()
    return res


def execute_many(sql, args):
    obj = MySQL.get_instance()
    row = obj.cursor.executemany(sql, args)
    obj.conn.commit()
    obj.conn.close()
    return row


def tx_execute(sqls, args):
    res = None
    obj = MySQL.get_instance()
    try:
        for i in range(0, len(sqls)):
            obj.cursor.execute(sqls[i], args[i])
        res = obj.cursor.fetchall()
        obj.conn.commit()
    except Exception as e:
        obj.conn.rollback()
    obj.conn.close()
    return res


def execute(sql, *args):
    obj = MySQL.get_instance()
    row = obj.cursor.execute(sql, *args)
    obj.conn.commit()
    obj.conn.close()
    return row


def query_dict(sql, *args):
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
    index_dict = dict()
    index = 0
    for desc in cursor.description:
        index_dict[desc[0]] = index
        index = index + 1
    return index_dict
