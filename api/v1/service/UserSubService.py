import datetime
import uuid

from api.v1.lib.common.mysqlsingle import execute, query_dict, query


def add_user_sub(type, receiver_user, msg, action, send_user, send_time, msg_type):
    sql = f"insert into TB_Msg_Queue(uuid,type,receive_user,msg,action,send_user,send_time,status,create_time,msg_type) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", type, receiver_user, msg, action, send_user, send_time, 'pending',
                         datetime.datetime.now(), msg_type))


def get_all_sub_user(symbol, platform):
    sql = "select distinct user_id from TB_User_Sub where symbol=%s and platform=%s"
    return query_dict(sql, (symbol, platform))
