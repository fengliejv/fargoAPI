import datetime
import uuid

from api.v1.lib.common.mysqlsingle import query_dict, execute
from api.v1.lib.common.utils import filterNull


def add_file_question(files_id, question, lang, version, user_id):
    sql = f"insert into TB_File_Question(uuid,files_id,question,create_time,user_id,lang,version) values (%s,%s,%s,%s,%s,%s,%s)"
    return execute(sql,
                   (f'{uuid.uuid1()}', files_id, f"{filterNull(question)}", datetime.datetime.now(), user_id, lang,
                    version))

def add_file_question_uuid(question_uuid,files_id, question, lang, version, user_id):
    sql = f"insert into TB_File_Question(uuid,files_id,question,create_time,user_id,lang,version) values (%s,%s,%s,%s,%s,%s,%s)"
    return execute(sql,
                   (f'{question_uuid}', files_id, f"{filterNull(question)}", datetime.datetime.now(), user_id, lang,
                    version))

def get_file_question(files_id, version, user_id, lang):
    sql = f"select * from TB_File_Question where files_id=%s and user_id=%s and version=%s and lang=%s"
    return query_dict(sql, (files_id, user_id, version, lang))
