import datetime
import uuid

from lib.Common.mysqlsingle import execute, query_dict


def add_upload_record(request_url, file_path, res):
    sql = f"insert into TB_File_Parsing_Upload(uuid,request_url,file_path,res) values ('{uuid.uuid1()}','{request_url}','{file_path}','{res}')"
    return execute(sql)


def add_parsing_record(req, result, parsing_platform, file_id, article_id):
    sql = f"insert into TB_File_Parsing(uuid,req,result,time,parsing_platform,file_id,article_id) values ('{uuid.uuid1()}','{req}','','{datetime.datetime.now()}','{parsing_platform}','{file_id}','{article_id}')"
    return execute(sql)


def get_file_title_limit_time(start_time):
    sql = f"select uuid,article_id,source,title from TB_File_Basic where uuid not in " \
          f"(select file_id from TB_File_Title where create_time>'{start_time}' and lang='zh-CN') and create_time>'{start_time}'"
    data = query_dict(sql)
    return data


def get_same_article_title(article_id, source):
    sql = f"select title from TB_File_Title where file_id in (select uuid from TB_File_Basic where " \
          f"article_id='{article_id}' and source='{source}')"
    data = query_dict(sql)
    return data
