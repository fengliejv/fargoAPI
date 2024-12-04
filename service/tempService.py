import datetime
import uuid

from lib.Common.mysqlsingle import execute, query_dict


def add_upload_record(request_url, file_path, res):
    sql = f"insert into TB_File_Parsing_Upload(uuid,request_url,file_path,res) values ('{uuid.uuid1()}','{request_url}','{file_path}','{res}')"
    return execute(sql)


def add_parsing_record(req, result, parsing_platform, file_id, article_id):
    sql = f"insert into TB_File_Parsing(uuid,req,result,time,parsing_platform,file_id,article_id) values ('{uuid.uuid1()}','{req}','','{datetime.datetime.now()}','{parsing_platform}','{file_id}','{article_id}')"
    return execute(sql)


def get_upload_rescord():
    sql = f"select * from TB_File_Parsing_Upload where request_url like '%%ocr=%%'"
    data = query_dict(sql)
    return data


def get_parsing_rescord():
    sql = f"select * from TB_File_Parsing order by time desc limit 6"
    data = query_dict(sql)
    return data
