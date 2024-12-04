import datetime
import uuid

from pymysql.converters import escape_string

from api.v1.lib.common.mysqlsingle import query_dict, execute


def get_all_wechat_user_sub(symbol):
    sql = f"select * from TB_User_Sub where symbol=%s"
    data = query_dict(sql, (symbol,))
    return data


def add_wechat_post_task(user_id, file_id):
    sql = f"insert into TB_Wechat_Report(uuid,user_id,file_id,status,update_time) values (%s,%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", user_id, file_id, '0', datetime.datetime.now()))


def set_wechat_post_task():
    sql = f"update TB_Wechat_Report set status='1' where 1=1"
    return execute(sql)


def get_wechat_user(name, sign, location):
    
    
    
    
    
    
    if location:
        sql = """
            SELECT * FROM TB_User
            WHERE (nick_name=%s AND sign=%s)
            OR (nick_name=%s AND location=%s)
            OR (sign=%s AND location=%s)
        """
        data = query_dict(sql, (
            escape_string(name), escape_string(sign), escape_string(name), escape_string(location), escape_string(sign),
            escape_string(location)))
    else:
        sql = """SELECT * FROM TB_User WHERE nick_name=%s """
        data = query_dict(sql, (name,))
    return data


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
    sql = f"delete from TB_User_Sub where user_id=%s and symbol=%s"
    execute(sql, (user_id, symbol))
    sql = f"insert into TB_User_Sub(uuid,user_id,symbol,update_time) values (%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", user_id, symbol, datetime.datetime.now()))
