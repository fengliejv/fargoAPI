import datetime
import uuid

from pymysql.converters import escape_string

from api.v1.lib.common.mysqlsingle import execute, query_dict, query


def set_file_param(param, value, file_path):
    sql = f"update TB_File set {param}=%s where file_path=%s"
    data = execute(sql, (value, file_path))
    return data


def add_info_log(source="sys", message=""):
    sql = f"insert into TB_CommonLog(level,source,message,create_time) values ('info','{source}','{escape_string(message)}','{datetime.datetime.now()}')"
    execute(sql)


def add_error_log(message="", source="sys", e=None):
    if e:
        e_msg = f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
    else:
        e_msg = message
    sql = f"insert into TB_CommonLog(level,source,message,create_time) values ('error','{source}','{escape_string(e_msg)}','{datetime.datetime.now()}')"
    execute(sql)


def add_fatal_log(message="", source="sys", e=None):
    if e:
        e_msg = f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
    else:
        e_msg = message
    sql = f"insert into TB_CommonLog(level,source,message,create_time) values ('fatal','{source}','{escape_string(e_msg)}','{datetime.datetime.now()}')"
    execute(sql)


def get_question_by_question_id(question_id):
    sql = f"select * from TB_File_Question where uuid=%s"
    data = query_dict(sql, (question_id,))
    return data


def get_report_question_by_userid(user_id):
    sql = f"select * from TB_File_Question where user_id=%s"
    data = query_dict(sql, (user_id,))
    return data


def get_user_report_question(user_id, files_id):
    sql = f"select * from TB_File_Question where user_id=%s and files_id=%s"
    data = query_dict(sql, (user_id, files_id))
    return data


def get_report_question_by_fileid(files_id):
    sql = f"select * from TB_File_Question where files_id=%s"
    data = query_dict(sql, (files_id,))
    return data


def get_question_by_file_id(files_id):
    sql = f"select * from TB_File_Question where files_id=%s"
    data = query_dict(sql, (files_id,))
    return data


def get_title_lang(file_id, lang):
    sql = f"select * from TB_File_Title where file_id=%s and lang=%s"
    data = query_dict(sql, (file_id, lang))
    return data


def add_title(file_id, title, lang):
    sql = f"insert into TB_File_Title(uuid,file_id,title,lang,create_time) values (%s,%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", file_id, f'{escape_string(title)}', lang, datetime.datetime.now()))


def add_question(question_id, reports_id, user_id, question, source):
    sql = f"insert into TB_File_Question (uuid,files_id,user_id,question,create_time,source) values (%s,%s,%s,%s,%s,%s)"
    return execute(sql, (f"{question_id}", reports_id, user_id, question, datetime.datetime.now(), source))


def add_answer(question_id, answer, source, lang="zh-CN"):
    sql = f"insert into TB_File_Answer (uuid,question_uuid,answer,lang,source,create_time) values (%s,%s,%s,%s,%s,%s)"
    return execute(sql,
                   (f"{uuid.uuid1()}", question_id, f'{escape_string(answer)}', lang, source, datetime.datetime.now()))


def update_answer_by_id(question_id, answer):
    sql = f"update TB_File_Question set content=%s where uuid=%s "
    return execute(sql, (answer, question_id))


def get_user(platform, user_id):
    sql = f"select * from TB_API_User where platform=%s and p_user_id=%s"
    return execute(sql, (platform, user_id))


def get_platform_user_id(p_user_id, platform):
    sql = f"select * from TB_API_User where p_user_id=%s and platform=%s"
    data = query_dict(sql, (p_user_id, platform))
    if data[0]:
        return data[0]['p_user_id']
    else:
        return ""


def get_upload_id_by_report_id(report_id):
    sql = f"select * from TB_API_ChatDoc where file_id=%s"
    return execute(sql, (report_id,))


def get_file_path_by_file_id(file_id):
    sql = f"select * from TB_File_Basic where uuid=%s"
    data = query_dict(sql, (file_id,))
    return data


def get_file_basic_by_file_id(file_id):
    sql = f"select uuid,local_save_path,article_id,title,handle_status,symbol,source,publish_time from TB_File_Basic where uuid=%s"
    data = query_dict(sql, (file_id,))
    return data


def get_file_basic_by_time(time):
    sql = f"select uuid,local_save_path,article_id,title,handle_status from TB_File_Basic where uuid=%s"
    data = query_dict(sql, (time,))
    return data


def get_file_basic_not_handle(create_time, source='gs'):
    sql = f"SELECT article_id,ANY_VALUE(local_save_path) AS local_save_path,ANY_VALUE(create_time) AS create_time," \
          f"ANY_VALUE(SOURCE) AS source,ANY_VALUE(original_url) AS original_url,ANY_VALUE(title) AS title," \
          f"ANY_VALUE(UUID) AS uuid,ANY_VALUE(publish_time) AS publish_time,group_concat(symbol Separator ',') as symbol " \
          f"from TB_File_Basic group by article_id HAVING source='{source}' AND create_time>'{create_time}' ORDER BY create_time ASC"
    data = query_dict(sql)
    return data


def get_file_basic_not_handle_by_time(publish_time_start, publish_time_end, source):
    sql = f"SELECT article_id,ANY_VALUE(local_save_path) AS local_save_path,ANY_VALUE(create_time) AS create_time," \
          f"ANY_VALUE(handle_status) AS handle_status,ANY_VALUE(source) AS source,ANY_VALUE(original_url) AS original_url,ANY_VALUE(title) AS title," \
          f"ANY_VALUE(UUID) AS uuid,ANY_VALUE(publish_time) AS publish_time,group_concat(symbol Separator ',') as symbol " \
          f"from TB_File_Basic group by article_id HAVING handle_status=0 AND publish_time>'{publish_time_start}' and publish_time<'{publish_time_end}' and source='{source}' ORDER BY create_time ASC"
    data = query_dict(sql)
    return data


def get_file_by_file_id(file_id):
    if len(file_id) == 4:
        sql = f"select * from TB_File_Basic where code=%s limit 1"
        return query_dict(sql, (file_id,))
    sql = f"select * from TB_File_Basic where uuid=%s limit 1"
    return query_dict(sql, (file_id,))


def get_files_brief(files_id):
    placeholders = ', '.join(['%s'] * len(files_id))
    sql = f"SELECT * FROM TB_File_Basic WHERE uuid IN ({placeholders})"
    return query_dict(sql, files_id)


def get_file_basic_embedding(file_id):
    sql = f"select a.uuid,a.source,a.local_save_path,a.create_time,b.version,a.embedding_status from TB_File_Basic as a inner join " \
          f"TB_File_Parsing as b on a.uuid=b.file_id where a.uuid=%s " \
          f"and (embedding_status='pending' or embedding_status='embedding_fail' or embedding_status is null or embedding_status='embedding_ok') order by a.create_time"
    data = query_dict(sql, (file_id,))

    return data


def get_attribute_by_download_file_id(file_id):
    sql = f"select * from TB_File where id=%s limit 1"
    return query_dict(sql, (file_id,))


def get_parsed_summary(file_id):
    sql = f"select * from TB_File_Parsed_Summary where file_id=%s order by create_time desc"
    data = query_dict(sql, (file_id,))
    return data


def get_parsed_summary_by_article_id(article_id):
    sql = f"select * from TB_File_Parsed_Summary where file_id in (select uuid from TB_File_Basic where article_id=%s) order by create_time desc"
    data = query_dict(sql, (article_id,))
    return data


def get_titles(file_id):
    sql = f"select * from TB_File_Title where file_id=%s"
    data = query_dict(sql, (file_id,))
    return data


def get_parsed_summary_lang(file_id, lang, version):
    sql = f"select * from TB_File_Parsed_Summary where file_id=%s and lang=%s and version=%s"
    data = query_dict(sql, (file_id, lang, version))
    return data


def get_file_symbols(article_id, source):
    sql = f"select symbol from TB_File_Basic where article_id=%s and source=%s"
    data = query_dict(sql, (article_id, source))
    return data


def search_file_code(code):
    sql = f"select code from TB_File_Basic where code=%s"
    data = query_dict(sql, (code,))
    return data


def set_file_code(article_id, source, code):
    sql = f"update TB_File_Basic set code=%s where article_id=%s and source=%s"
    return execute(sql, (code, article_id, source))


def get_file_tickers(article_id, source):
    sql = f"select symbol from TB_File_Basic where article_id=%s and source=%s"
    data = query_dict(sql, (article_id, source))
    tickers = []
    for t in data:
        if not t['symbol'] in tickers:
            tickers.append(t['symbol'])
    return tickers


def get_search_file(start_time, end_time, symbol, title, source, count, code, type):
    if code:
        sql = """
                select type,parse_status,embedding_status,code,article_id,a.uuid,source,local_save_path,publish_time,symbol,a.title,b.title as cn_title from 
                TB_File_Basic as a left join TB_File_Title as b on a.uuid=b.file_id 
                where publish_time>%s and publish_time<%s and code=%s order by a.create_time desc limit 1"""
        param = (start_time, end_time, code)
        data = query_dict(sql, param)
        return data
    param = ()
    sql = """
        select type,parse_status,embedding_status,code,article_id,a.uuid,source,local_save_path,publish_time,symbol,a.title,b.title as cn_title from 
        TB_File_Basic as a left join TB_File_Title as b on a.uuid=b.file_id 
        where publish_time>%s and publish_time<%s """
    if type:
        sql = sql + " and type='pdf' "
    fliter = None
    if symbol and title and source:
        fliter = """and symbol=%s and (a.title like "%%"%s"%%" or b.title like "%%"%s"%%") and source=%s order by a.create_time desc limit %s"""
        param = (start_time, end_time, symbol, title, title, source, count)
    elif not symbol and title and source:
        fliter = """and (a.title like "%%"%s"%%" or b.title like "%%"%s"%%") and source=%s order by a.create_time desc limit %s"""
        param = (start_time, end_time, title, title, source, count)
    elif symbol and not title and source:
        fliter = """and symbol=%s and source=%s order by a.create_time desc limit %s"""
        param = (start_time, end_time, symbol, source, count)
    elif symbol and title and not source:
        fliter = """and symbol=%s and (a.title like "%%"%s"%%" or b.title like "%%"%s"%%") order by a.create_time desc limit %s"""
        param = (start_time, end_time, symbol, title, title, count)
    elif symbol and not title and not source:
        fliter = """and symbol=%s order by a.create_time desc limit %s"""
        param = (start_time, end_time, symbol, count)
    elif not symbol and title and not source:
        fliter = """and (a.title like "%%"%s"%%" or b.title like "%%"%s"%%") order by a.create_time desc limit %s"""
        param = (start_time, end_time, title, title, count)
    elif not symbol and not title and source:
        fliter = """and source=%s order by a.create_time desc limit %s"""
        param = (start_time, end_time, source, count)
    elif not symbol and not title and not source:
        fliter = """ order by a.create_time desc limit %s"""
        param = (start_time, end_time, count)
    sql = sql + fliter
    data = query_dict(sql, param)
    return data


def get_report_list(start_time, end_time, page_count, page_size):
    sql = """
        SELECT article_id,ANY_VALUE(SOURCE) AS source,ANY_VALUE(title) AS title,ANY_VALUE(UUID) AS uuid,
        ANY_VALUE(publish_time) AS publish_time,group_concat(symbol Separator ',') as symbol
        FROM TB_File_Basic group by article_id
        HAVING publish_time BETWEEN %s AND %s and symbol is not null
        ORDER BY publish_time
        LIMIT %s OFFSET %s;
    """
    data = query_dict(sql, (start_time, end_time, page_size, (page_size - 1) * page_count))
    return data


def get_research_list(start_time, end_time, page_count, page_size):
    sql = """
        SELECT uuid,publish_time,source,title,file_type,lang,source_url,download_status,meta_data 
        FROM TB_Research where publish_time BETWEEN %s AND %s ORDER BY publish_time 
        LIMIT %s OFFSET %s;
    """
    data = query_dict(sql, (start_time, end_time, page_size, (page_size - 1) * page_count))
    return data
