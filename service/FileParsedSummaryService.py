import datetime
import uuid

from lib.Common.mysqlsingle import execute, query_dict


def add_upload_record(request_url, file_path, res):
    sql = f"insert into TB_File_Parsing_Upload(uuid,request_url,file_path,res) values ('{uuid.uuid1()}','{request_url}','{file_path}','{res}')"
    return execute(sql)


def add_parsing_record(req, result, parsing_platform, file_id, article_id):
    sql = f"insert into TB_File_Parsing(uuid,req,result,time,parsing_platform,file_id,article_id) values ('{uuid.uuid1()}','{req}','','{datetime.datetime.now()}','{parsing_platform}','{file_id}','{article_id}')"
    return execute(sql)


def get_file_summary_limit_time(start_time):
    sql = f"select uuid,article_id,source,title from TB_File_Basic where uuid not in " \
          f"(select file_id from TB_File_Parsed_Summary where create_time>'{start_time}') " \
          f"and create_time>'{start_time}' and embedding_status='embedding_ok' and publish_time>CURDATE() - INTERVAL 1 DAY "
    data = query_dict(sql)
    return data


def get_file_summary_limit_recently(start_time):
    sql = f"select uuid,article_id,source,title from TB_File_Basic where uuid not in " \
          f"(select file_id from TB_File_Parsed_Summary where create_time>'{start_time}') " \
          f"and create_time>'{start_time}' and embedding_status='embedding_ok'"
    data = query_dict(sql)
    return data


def get_same_article_summary(article_id, source, lang):
    sql = f"select summary from TB_File_Parsed_Summary where file_id in (select uuid from TB_File_Basic where " \
          f"article_id='{article_id}' and source='{source}' and lang='{lang}') and lang='{lang}' limit 1"
    data = query_dict(sql)
    return data


def get_same_article_origin_summary(article_id, source, lang):
    sql = f"select summary from TB_File_Parsed_Original_Summary where file_id in (select uuid from TB_File_Basic where " \
          f"article_id='{article_id}' and source='{source}' and lang='{lang}') and lang='{lang}' limit 1"
    data = query_dict(sql)
    return data
