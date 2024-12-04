import datetime

from pymysql.converters import escape_string

from lib.mysql import execute

def info(source="sys", message=""):
    sql = f"insert into TB_CommonLog(level,source,message,create_time) values ('info','{source}','{escape_string(message)}','{datetime.datetime.now()}')"
    execute(sql)

def error(message="", source="sys", e=None):
    if e:
        e_msg = f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
    else:
        e_msg = message
    sql = f"insert into TB_CommonLog(level,source,message,create_time) values ('error','{source}','{escape_string(e_msg)}','{datetime.datetime.now()}')"
    execute(sql)

def fatal(message="", source="sys", e=None):
    if e:
        e_msg = f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
    else:
        e_msg = message
    sql = f"insert into TB_CommonLog(level,source,message,create_time) values ('fatal','{source}','{escape_string(e_msg)}','{datetime.datetime.now()}')"
    execute(sql)
