import datetime
import uuid

from api.v1.lib.common.mysqlsingle import execute, query_dict, query


def get_platform(user: str):
    sql = f"select * from TB_API_Platform where user=%s"
    data = query_dict(sql, (user,))
    return data


def add_user(platform, user_id):
    sql = f"insert into TB_API_User (uuid,p_user_id,platform,preference) values (%s,%s,%s,%s)"
    return execute(sql, (f'{uuid.uuid1()}', user_id, platform, '{"v1":0.5,"v2":0.5,"v3":0.5,"v4":0.5,"v5":0.5}'))


def get_user(platform, user_id):
    sql = f"select * from TB_API_User where platform=%s and p_user_id=%s"
    return query_dict(sql, (platform, user_id))


def get_preference(platform, user_id):
    sql = f"select * from TB_API_User where platform=%s and p_user_id=%s"
    return query_dict(sql, (platform, user_id))


def set_preference(platform, user_id, preference):
    sql = f"update TB_API_User set preference=%s where platform=%s and p_user_id=%s"
    return execute(sql, (preference, platform, user_id))
