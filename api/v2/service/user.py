import datetime

from lib.mysql import query_dict, execute


def get_platform_by_id(uuid: str):
    sql = f"select * from tb_platform where uuid=%s"
    data = query_dict(sql, (uuid,))
    return data


def get_user_by_id(uuid: str):
    sql = f"select * from tb_user where uuid=%s"
    data = query_dict(sql, (uuid,))
    return data


def get_user_by_platform_phone(platform_id, phone):
    sql = f"select * from tb_user where platform_id=%s and phone=%s"
    data = query_dict(sql, (platform_id, phone))
    return data


def add_user(user_id, platform_id, phone=None):
    datetime_now = datetime.datetime.now()
    sql = f"insert into tb_user(uuid,platform_id,phone,created_time,updated_time) values (%s,%s,%s,%s,%s)"
    return execute(sql, (user_id, platform_id, phone, datetime_now, datetime_now))
