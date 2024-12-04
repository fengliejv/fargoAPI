import datetime
import uuid

from lib.Common.mysqlsingle import query_dict, execute


def get_not_upload_file():
    sql = f"select * from TB_File_Parsed_Summary"
    data = query_dict(sql)
    return data


def get_not_generation_summary_file():
    sql = f"select result,file_id,version from TB_File_Parsing where file_id not in (select file_id from TB_File_Parsed_Summary)"
    data = query_dict(sql)
    return data


def get_not_generation_summary_file_title(file_id):
    sql = f"select title from TB_File_Basic where uuid=%s limit 1"
    data = query_dict(sql, (file_id,))
    if data[0]:
        return data[0]['title']
    else:
        return ""


def add_summary(summary, file_id, lang="en"):
    sql = f"insert into TB_File_Parsed_Summary(uuid,summary,file_id,lang,create_time) values (%s,%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", summary, file_id, lang, datetime.datetime.now()))
