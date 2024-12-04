import psycopg2
from config.common import Config


class Postgres(object):
    _instance = None

    @staticmethod
    def get_instance():
        if Postgres._instance is None:
            Postgres._instance = Postgres()
        return Postgres._instance

    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                host=Config.DB_POSTGRES_HOST,
                port=Config.DB_POSTGRES_PORT,
                user=Config.DB_POSTGRES_USERNAME,
                password=Config.DB_POSTGRES_PASSWORD,
                dbname=Config.DB_POSTGRES_DATABASE
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")

def tx_execute(sqls, args):
    res = None
    obj = Postgres.get_instance()
    try:
        for i in range(0, len(sqls)):
            obj.cursor.execute(sqls[i], args[i])
        res = obj.cursor.fetchall()
        obj.conn.commit()
    except Exception as e:
        obj.conn.rollback()
    obj.conn.close()
    return res

def query(sql, *args):
    obj = Postgres.get_instance()
    obj.cursor.execute(sql, *args)
    res = obj.cursor.fetchall()
    obj.conn.close()
    return res


def execute_many(sql, args):
    obj = Postgres.get_instance()
    row = obj.cursor.executemany(sql, args)
    obj.conn.commit()
    obj.conn.close()
    return row


def execute(sql, *args):
    obj = Postgres.get_instance()
    row = obj.cursor.execute(sql, *args)
    obj.conn.commit()
    obj.conn.close()
    return row


def query_dict(sql, *args):
    obj = Postgres.get_instance()
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
    obj = Postgres.get_instance()
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


if __name__ == "__main__":
    db = Postgres.get_instance()
    cursor = db.cursor
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"PostgreSQL version: {version}")
