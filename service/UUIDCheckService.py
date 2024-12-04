import datetime

from lib.Common.mysqlsingle import query_dict, execute


def get_uuid_check(id):
    sql = f"select * from TB_UUID_Check where uuid='{id}'"
    data = query_dict(sql)
    return data


def add_uuid_check(id):
    sql = f"insert into TB_UUID_Check(uuid,create_time) values ('{id}','{datetime.datetime.now()}')"
    return execute(sql)
