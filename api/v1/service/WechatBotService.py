import datetime
import uuid

from pymysql.converters import escape_string

from api.v1.lib.common.mysqlsingle import execute, query_dict, query, execute_many


def get_all_wechat_user():
    sql = f"select * from TB_User"
    data = query_dict(sql)
    return data


def add_wechat_user(name, location, sign):
    sql = f"insert into TB_User (uuid,nick_name,location,sign,update_time) values (%s,%s,%s,%s,%s)"
    return execute(sql, (f'{uuid.uuid1()}', name, location, escape_string(sign), datetime.datetime.now()))


def update_wechat_user(user_id, name, location, sign):
    sql = f"update TB_User set nick_name=%s,sign=%s,location=%s,update_time=%s where uuid=%s"
    return execute(sql, (name, escape_string(sign), location, user_id, datetime.datetime.now()))


def get_all_not_send_report():
    sql = f"select b.nick_name,b.sign,b.location,a.file_id,b.uuid,a.uuid as id from TB_Wechat_Report as a inner join TB_User as b on a.user_id=b.uuid where a.status=0"
    data = query_dict(sql)
    return data


def get_not_send_report(user_id):
    sql = f"select b.nick_name,b.sign,b.location,a.file_id from TB_Wechat_Report as a inner join TB_User as b on a.user_id=b.uuid and b.uuid=%s  where a.status=0"
    data = query_dict(sql, (user_id,))
    return data


def get_send_report_source(file_id):
    sql = f"select title,source,local_save_path,publish_time,symbol from TB_File_Basic where uuid=%s"
    data = query_dict(sql, (file_id,))
    return data


def get_newest_report_by_symbol(symbol):
    sql = f"select uuid,title,source,local_save_path,publish_time,symbol from TB_File_Basic where symbol=%s order by publish_time desc limit 1"
    data = query_dict(sql, (symbol,))
    return data

def set_send_history(history_param):
    sql = f"update TB_Wechat_Report set status=1,send_time=%s where status=0 and uuid=%s"
    return execute_many(sql, history_param)
