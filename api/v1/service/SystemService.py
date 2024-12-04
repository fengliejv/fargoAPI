import datetime
import uuid

from api.v1.lib.common.mysqlsingle import execute,query_dict, query


def set_system_variable(key, value):
    sql = f"update TB_Config set value=%s where name=%s"
    data = execute(sql, (value, key))
    return data

def get_system_variable(key):
    sql = f"SELECT * FROM TB_Config WHERE name=%s limit 1"
    data = query_dict(sql, (key,))
    return data
