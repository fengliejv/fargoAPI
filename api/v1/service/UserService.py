import datetime
import uuid

from api.v1.lib.common.mysqlsingle import execute, query_dict, query


def get_user(key_words):
    sql = f"select * from TB_User where nick_name=%s"
    return query_dict(sql, (key_words,))
