import datetime
import uuid

from pymysql.converters import escape_string

from lib.Common.mysqlsingle import query_dict, execute


def get_all_wechat_user_sub(symbol):
    sql = f"select * from TB_User_Sub where symbol=%s"
    data = query_dict(sql, (symbol,))
    return data


def add_wechat_post_task(user_id, file_id):
    sql = f"insert into TB_Wechat_Report(uuid,user_id,file_id,status,update_time) values (%s,%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", user_id, file_id, '0', datetime.datetime.now()))


def add_wechat_msg_queue(receiver_user, msg, send_user, action, send_time):
    sql = f"insert into TB_Wechat_Msg_Queue(uuid,receiver_user,msg,send_user,action,status,send_time,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
    return execute(sql, (
        f"{uuid.uuid1()}", receiver_user, escape_string(msg), send_user, action, '0', send_time,
        datetime.datetime.now()))


def get_wechat_msg_queue(receiver_user, action):
    sql = f"select DISTINCT uuid from TB_Wechat_Msg_Queue where receiver_user=%s and action=%s"
    data = query_dict(sql, (receiver_user, action))
    return data


def get_wechat_post_task(user_id, file_id):
    sql = f"select DISTINCT file_id from TB_Wechat_Report where file_id=%s and user_id=%s"
    data = query_dict(sql, (user_id, file_id))
    return data


def set_wechat_post_task():
    sql = f"update TB_Wechat_Report set status='1' where 1=1"
    return execute(sql)


def get_all_symbol():
    sql = f"select DISTINCT symbol,company_name from TB_Script_CompanyNameCode"
    data = query_dict(sql)
    return data


def set_cancel_symbol(user_id, symbol):
    sql = f"delete from TB_User_Sub where user_id=%s and symbol=%s"
    return execute(sql, (user_id, symbol))


def get_search_symbol(key_word):
    sql = f'select DISTINCT symbol from TB_Script_CompanyNameCode where company_name like "%%"%s"%%" or symbol=%s'
    data = query_dict(sql, (key_word, key_word))
    return data


def get_search_sub(user_id):
    sql = f'select DISTINCT symbol from TB_User_Sub where user_id=%s'
    data = query_dict(sql, (user_id,))
    return data


def add_new_sub(user_id, symbol):
    sql = f"insert into TB_User_Sub(uuid,user_id,symbol,update_time) values (%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", user_id, symbol, datetime.datetime.now()))


def get_basic_title_summary(start_time):
    sql = """
        select a.info,a.uuid,a.article_id,a.symbol,a.publish_time,a.source,a.title,b.title as title_cn,c.summary from 
        ((select info,symbol,publish_time,source,title,uuid,article_id 
        from TB_File_Basic where create_time>%s and info is not null) a 
        inner join 
        (select file_id,title from TB_File_Title where create_time>%s and lang='zh-CN') b
        on a.uuid=b.file_id)
        inner join
        (select file_id,summary from TB_File_Parsed_Summary where create_time>%s and lang='zh-CN') c
        on a.uuid=c.file_id
        """
    data = query_dict(sql, (start_time, start_time, start_time))
    return data
