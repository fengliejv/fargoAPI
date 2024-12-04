import datetime
import uuid

from pymysql.converters import escape_string

from api.v1.lib.common.mysqlsingle import execute, query_dict, query


def get_not_generation_summary_file_title(file_id):
    sql = f"select title from TB_File_Basic where uuid=%s limit 1"
    data = query_dict(sql, (file_id,))
    return data


def add_summary(summary, file_id, version, lang="en"):
    sql = f"insert into TB_File_Parsed_Summary(uuid,summary,file_id,lang,create_time,version) values (%s,%s,%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", summary, file_id, lang, datetime.datetime.now(), version))


def add_original_brief(summary, file_id, version, lang="en"):
    sql = f"insert into TB_File_Parsed_Original_Summary(uuid,summary,file_id,lang,create_time,version) values (%s,%s,%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", summary, file_id, lang, datetime.datetime.now(), version))


def set_file_basic_status(file_id, status):
    sql = f"update TB_File_Basic set handle_status=%s where uuid=%s"
    return execute(sql, (status, file_id))





