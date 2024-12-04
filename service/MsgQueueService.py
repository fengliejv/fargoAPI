import datetime
import uuid

from pymysql.converters import escape_string

from lib.Common.mysqlsingle import execute, query_dict


def add_msg_queue(type, receiver_user, msg, action, send_user, tag, msg_type):
    sql = f"insert into TB_Msg_Queue(uuid,type,receive_user,msg,action,send_user,tag,status,create_time,msg_type) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", type, receiver_user, msg, action, send_user, tag, 'pending',
                         datetime.datetime.now(), msg_type))


def get_msg_queue(type, action, create_time):
    sql = "select receive_user,tag from TB_Msg_Queue where type=%s and action=%s and create_time>%s"
    return query_dict(sql, (type, action, create_time))


def get_msg_queue_pending(type, now_time):
    sql = """
    select a.msg_type,a.action,a.uuid,receive_user,msg,b.nick_name from TB_Msg_Queue as a inner join 
    TB_User as b on a.receive_user=b.uuid where type=%s and send_time<=%s and (status='pending' or status='send_fail')
    """
    return query_dict(sql, (type, now_time))


def set_msg_queue_attr(msg_id, attr, value):
    sql = f"update TB_Msg_Queue set {attr}='{value}' where uuid='{msg_id}'"
    if attr == "status" and value == "send_ok":
        sql = f"update TB_Msg_Queue set {attr}='{value}',send_time='{datetime.datetime.now()}' where uuid='{msg_id}'"
    return execute(sql)

def set_ready_msg():

    sql = f"update TB_Msg_Queue set status='sending' where status='pending' and send_time<'{datetime.datetime.now()}'"
    return execute(sql)