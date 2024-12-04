import datetime
import uuid

from pymysql.converters import escape_string

from lib.mysql import query_dict, execute


def get_question_by_question_id(question_id):
    sql = f"select * from TB_File_Question where uuid=%s"
    data = query_dict(sql, (question_id,))
    return data


def add_question(question_id, reports_id, user_id, question, source):
    sql = f"insert into TB_File_Question (uuid,files_id,user_id,question,create_time,source) values (%s,%s,%s,%s,%s,%s)"
    return execute(sql, (f"{question_id}", reports_id, user_id, question, datetime.datetime.now(), source))


def add_answer(question_id, answer, source, lang="zh-CN"):
    sql = f"insert into TB_File_Answer (uuid,question_uuid,answer,lang,source,create_time) values (%s,%s,%s,%s,%s,%s)"
    return execute(sql,
                   (f"{uuid.uuid1()}", question_id, f'{escape_string(answer)}', lang, source, datetime.datetime.now()))
