import datetime
import uuid

from pymysql.converters import escape_string

from config import Config
from lib.Common.mysqlsingle import query, execute, query_dict, query_key_dict
from lib.Common.utils import getTimeNode, filterNull, strbin


def get_file_basic_not_parse_uuid_by_source(source="gs"):
    sql = f"select a.uuid,a.local_save_path from TB_File_Basic as a where " \
          f"uuid not in (select file_basic_id from TB_File_Basic_TickerView) " \
          f"and source='{source}' and publish_time>'{getTimeNode()}'"
    data = query_dict(sql)
    return data


def get_question_not_answered_by_source(source="gs"):
    sql = f"select b.uuid,b.question from (TB_File_Basic as a inner join TB_File_Basic_Summary_Question as b on a.uuid=b.file_basic_id) " \
          f"where b.uuid not in (select file_basic_content_summary_question_id from TB_File_ContentSummaryQuestion_Answer) and source='{source}' " \
          f"and publish_time>'2024-01-11 00:55:21' order by publish_time desc limit 100"
    data = query_dict(sql)
    return data


def get_question_not_created_uuid_by_source(source="gs"):
    sql = f"select a.uuid,b.summary from (TB_File_Basic as a inner join TB_File_Basic_Summary as b on a.uuid=b.file_basic_id) " \
          f"where a.uuid not in (select file_basic_id from TB_File_Basic_Summary_Question) and  source='{source}' and publish_time>'{getTimeNode()}' order by publish_time desc limit 100"
    data = query_dict(sql)
    return data


def get_file_basic_not_generate_summary_by_source(source="gs"):
    sql = f"select a.uuid,a.local_save_path from TB_File_Basic as a WHERE uuid NOT IN " \
          f"(SELECT file_basic_id FROM TB_File_Basic_Summary) and source='{source}' " \
          f"and publish_time>'{getTimeNode()}' order by publish_time desc limit 100"
    data = query_dict(sql)
    return data


def add_tb_file_basic_view_record(file_basic_id, ticker=None, raise_fall=None, target=None, rating=None,
                                  sn=None):
    sql = f"insert into TB_File_Basic_TickerView(uuid,file_basic_id,ticker,raise_fall,target,rating,sn,parse_time) " \
          f"values ('{uuid.uuid1()}',{filterNull(file_basic_id)},{filterNull(ticker)},{filterNull(raise_fall)},{filterNull(target)},{filterNull(rating)},{sn},'{datetime.datetime.now()}')"
    return execute(sql)


def add_tb_file_basic_summary_record(file_basic_id, summary):
    sql = f"insert into TB_File_Basic_Summary(uuid,file_basic_id,summary,create_time) " \
          f"values ('{uuid.uuid1()}','{file_basic_id}',{filterNull(summary)},'{datetime.datetime.now()}')"
    return execute(sql)


def add_question_answer_record(file_basic_content_summary_question_id, answer):
    sql = f"insert into TB_File_ContentSummaryQuestion_Answer(uuid,file_basic_content_summary_question_id,answer,create_time) " \
          f"values ('{uuid.uuid1()}','{file_basic_content_summary_question_id}',{filterNull(answer)},'{datetime.datetime.now()}')"
    return execute(sql)


def add_tb_file_basic_summary_question_record(file_basic_id, question, sn):
    sql = f"insert into TB_File_Basic_Summary_Question(uuid,file_basic_id,question,create_time,sn) " \
          f"values ('{uuid.uuid1()}','{file_basic_id}',{filterNull(question)},'{datetime.datetime.now()}',{sn})"
    return execute(sql)


def get_newest_tb_file_basic_by_source(source='gs'):
    sql = f'select article_id,publish_time,file_id from TB_File_Basic where source="{source}" order by publish_time desc limit 1'
    data = query_dict(sql)
    return data


def get_answer_not_sync(latest_time):
    sql = f"select a.symbol,a.uuid,q.question,n.answer,a.author,v.rating,a.publish_time,a.title,n.create_time,a.article_id from " \
          f"((TB_File_Basic as a left join TB_File_Basic_Summary_Question as q on a.uuid=q.file_basic_id) left join " \
          f"TB_File_ContentSummaryQuestion_Answer as n on n.file_basic_content_summary_question_id=q.uuid) left join " \
          f"TB_File_Basic_TickerView as v on a.uuid=v.file_basic_id where q.create_time>'{latest_time}' and publish_time>'2024-01-12 00:00:21' order by q.create_time asc"
    data = query_dict(sql)
    return data


def get_file_basic_not_sync(latest_time, source='sa'):
    sql = f"select a.uuid,a.article_id,a.symbol,a.title,a.content,a.publish_time,a.lang,a.source,b.summary,a.create_time from " \
          f"TB_File_Basic as a inner join TB_File_Basic_Summary as b on a.uuid=b.file_basic_id where a.create_time>'{latest_time}'" \
          f"and source='{source}' order by a.create_time asc"
    data = query_dict(sql)
    return data


def get_all_tb_file_basic_by_source(source, key):
    sql = f"select article_id,publish_time,file_id from TB_File_Basic where source='{source}' and publish_time>'{getTimeNode()}' order by publish_time desc"
    data = query_key_dict(sql=sql, key=key)
    return data


def get_full_tb_file_basic_by_source(source, key):
    sql = f"select article_id,publish_time,file_id from TB_File_Basic where source='{source}' order by publish_time desc"
    data = query_key_dict(sql=sql, key=key)
    return data


def get_all_tb_file_basic_view_by_source(source, key):
    sql = f"select file_basic_id,parse_time from TB_File_Basic where source='{source}' and publish_time>'{getTimeNode()}' order by parse_time desc"
    data = query_key_dict(sql=sql, key=key)
    return data


def check_repeat_report(source, article_id, symbol):
    sql = f"select * from TB_File_Basic where source='{source}' and article_id='{article_id}'"
    data = query_dict(sql)
    return data


def check_repeat_file(source, article_id):
    sql = f"select * from TB_File_Basic where source='{source}' and article_id='{article_id}'"
    data = query_dict(sql)
    return data


def get_all_tb_file_by_publish_time_and_source(publish_time, source, type='html'):
    sql = f'SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY publish_time DESC) AS row_num FROM TB_File) t ' \
          f'WHERE row_num = 1 and publish_time>"{publish_time}" and file_path like "%%/{source}/%%" and type="{type}" order by publish_time asc'
    data = query_dict(sql)
    return data


def get_need_format_file(source, type='pdf'):
    sql = """
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY id) AS rn
            FROM TB_File
            WHERE create_time >= CURDATE() - INTERVAL 1 DAY 
              AND create_time < CURDATE() + INTERVAL 1 DAY
              AND publish_time >= CURDATE() - INTERVAL 1 DAY 
              AND publish_time < CURDATE() + INTERVAL 1 DAY
              AND file_path like %s and type=%s
        ) AS b 
        WHERE rn = 1 
        AND file_path NOT IN (
           SELECT DISTINCT local_save_path
           FROM TB_File_Basic
           WHERE create_time >= CURDATE() - INTERVAL 1 DAY 
             AND create_time < CURDATE() + INTERVAL 1 DAY
             AND publish_time >= CURDATE() - INTERVAL 1 DAY 
             AND publish_time < CURDATE() + INTERVAL 1 DAY
        )
    """
    data = query_dict(sql, (f"%/{source}/%", type))
    return data


def get_need_format_file_recently(source, type='pdf'):
    sql = """
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY id) AS rn
            FROM TB_File
            WHERE create_time >= CURDATE() - INTERVAL 7 DAY 
              AND create_time < CURDATE() + INTERVAL 7 DAY
              AND file_path like %s and type=%s
        ) AS b 
        WHERE rn = 1 
        AND file_path NOT IN (
           SELECT DISTINCT local_save_path
           FROM TB_File_Basic
           WHERE create_time >= CURDATE() - INTERVAL 7 DAY 
             AND create_time < CURDATE() + INTERVAL 7 DAY
        )
    """
    data = query_dict(sql, (f"%/{source}/%", type))
    return data


def get_need_format_file_recently(source, type='pdf'):
    sql = """
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY id) AS rn
            FROM TB_File
            WHERE create_time >= CURDATE() - INTERVAL 7 DAY 
              AND create_time < CURDATE() + INTERVAL 7 DAY
              AND file_path like %s and type=%s
        ) AS b 
        WHERE rn = 1 
        AND file_path NOT IN (
           SELECT DISTINCT local_save_path
           FROM TB_File_Basic
           WHERE create_time >= CURDATE() - INTERVAL 7 DAY 
             AND create_time < CURDATE() + INTERVAL 7 DAY
        )
    """
    data = query_dict(sql, (f"%/{source}/%", type))
    return data


def add_tb_file_basic_record(article_id, file_id, author=None, source=None, title=None,
                             lang=None, original_url=None, local_save_path=None, publish_time=None, creator=None,
                             create_time=None, symbol=None):
    sql = f"insert into TB_File_Basic(uuid,article_id,file_id,author,source,title,lang,original_url,local_save_path, " \
          f"publish_time,creator,create_time,symbol) values ('{uuid.uuid1()}',{filterNull(article_id)},{filterNull(file_id)}," \
          f"{filterNull(author)},{filterNull(source)},{filterNull(title)}," \
          f"{filterNull(lang)},{filterNull(original_url)},{filterNull(local_save_path)},{filterNull(publish_time)},{filterNull(creator)},'{create_time}',{filterNull(symbol)})"
    return execute(sql)


def add_tb_file_basic(article_id, file_id, author=None, source=None, title=None,
                      lang=None, original_url=None, local_save_path=None, publish_time=None, creator=None,
                      create_time=None, symbol=None, attribute=None):
    sql = f"insert into TB_File_Basic(uuid,article_id,file_id,author,source,title,lang,original_url,local_save_path, " \
          f"publish_time,creator,create_time,symbol,attribute) values ('{uuid.uuid1()}',{filterNull(article_id)},{filterNull(file_id)}," \
          f"{filterNull(author)},{filterNull(source)},{filterNull(title)}," \
          f"{filterNull(lang)},{filterNull(original_url)},{filterNull(local_save_path)},{filterNull(publish_time)},{filterNull(creator)}," \
          f"'{create_time}',{filterNull(symbol)},'{attribute}')"
    return execute(sql)


def add_tb_file_basic_record_uuid(report_id, article_id, file_id, author=None, source=None,
                                  title=None,
                                  lang=None, original_url=None, local_save_path=None, publish_time=None, creator=None,
                                  create_time=None, symbol=None, type='pdf'):
    sql = f"insert into TB_File_Basic(uuid,article_id,file_id,author,source,title,lang,original_url,local_save_path, " \
          f"publish_time,creator,create_time,symbol,type) values ('{report_id}',{filterNull(article_id)},{filterNull(file_id)}," \
          f"{filterNull(author)},{filterNull(source)},{filterNull(title)}," \
          f"{filterNull(lang)},{filterNull(original_url)},{filterNull(local_save_path)},'{publish_time}'," \
          f"{filterNull(creator)},'{create_time}',{filterNull(symbol)},'{type}')"
    return execute(sql)


def get_all_company_code(type='gs'):
    """
    返回公司表数据
    :return:
    """
    sql = f"select en_name,company_code,symbol from TB_Company as a inner join TB_Script_CompanyNameCode as b on a.id=b.company_id where b.platform='{type}'"
    data = list(query(sql))
    return data


def get_all_company_symbol(type='gs'):
    """
    :return:
    """
    sql = f"select en_name,company_code,symbol from TB_Company as a inner join TB_Script_CompanyNameCode as b on a.id=b.company_id where b.platform='{type}'"
    data = query_dict(sql)
    return data


def add_file_record(action="", type="", file_path="", profile="", title="", source="",
                    creator="system", publish_time=datetime.datetime.now(), attribute="", symbol=""):
    sql = f"insert into TB_File(id,action,type,file_path,profile,title,source,creator,create_time,publish_time,attribute,symbol) values" \
          f" ('{uuid.uuid1()}','{escape_string(action)}','{type}','{file_path}','{escape_string(profile)}','{escape_string(title)}','{source}','{creator}','{datetime.datetime.now()}','{publish_time}','{attribute}','{symbol}')"
    return execute(sql)
    
    
    
    


def get_same_source(source):
    sql = "select distinct(id) from TB_File where source=%s"
    return query_dict(sql, (source,))


def get_file_basic_by_file_id(file_id):
    sql = f"select uuid,local_save_path,article_id,title,handle_status from TB_File_Basic where uuid=%s"
    data = query_dict(sql, (file_id,))
    return data


def add_info_log(message="", source="sys"):
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


def get_all_file_record():
    sql = f''
    execute(sql)


def get_file_record_by_type(type="marquee.gs.com"):
    sql = f"select * from TB_File where source like '%%{type}%%' order by publish_time desc"
    result = query_dict(sql)
    return result


def get_all_file_record():
    sql = f"select distinct file_path,title from TB_File "
    result = query_dict(sql)
    return result


def get_article_newest_time_by_company(platform="sa", company_code="JD"):
    sql = f"select * from TB_File where file_path like '%%{platform}/{company_code}%%' order by publish_time desc limit 1"
    result = query_dict(sql)
    return result


def get_article_newest_time_by_symbol(symbol="", platform="sa"):
    sql = f"select * from TB_File where symbol='{symbol}' and file_path like '%%/{platform}/%%' order by publish_time desc limit 1"
    result = query_dict(sql)
    return result


def get_article_newest_time_sub(platform="sa"):
    sql = f"select * from TB_File where file_path like '%%/{platform}/%%' and (symbol is null or symbol='') order by publish_time desc limit 1 "
    result = query_dict(sql)
    return result


def get_file_by_time_platform_company_code(platform="sa", company_code="JD", latest_time=datetime.datetime.now()):
    sql = f"select * from TB_File where file_path like '%%{platform}/{company_code}%%' and publish_time>{latest_time} order by publish_time desc"
    print(sql)
    result = query_dict(sql)
    return result


def get_file_basic_not_handle():
    sql = f"select uuid from TB_File_Basic where handle_status=%s and create_time>%s"
    data = query_dict(sql, (False, datetime.datetime.now() - datetime.timedelta(hours=24)))
    return data
