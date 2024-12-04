import datetime
import uuid

from lib.Common.mysqlsingle import execute, query_dict


def get_research_newest(platform="ms"):
    sql = f"select * from TB_Research where source=%s order by publish_time desc limit 1"
    result = query_dict(sql, (platform,))
    return result


def get_research_recently(platform="ms", start_time=None):
    sql = f"select * from TB_Research where source=%s and publish_time>%s order by publish_time desc"
    result = query_dict(sql, (platform, start_time))
    return result


def add_research(p_key=None, publish_time=None, source=None, title=None,
                 tags=None, author=None, file_type=None,
                 source_url=None, lang=None, meta_data=None, download_status=False,
                 create_time=datetime.datetime.now(), event_id=None, business_type=None, parse_status=None):
    sql = """
    insert into TB_Research(uuid, publish_time, source, title, tags, author, file_type, download_status,
    source_url, meta_data, lang, create_time,event_id,business_type,parse_status) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    return execute(sql, (p_key, publish_time, source, title,
                         tags, author, file_type, download_status, source_url, meta_data, lang,
                         create_time, event_id, business_type, parse_status))


def add_research_attribute(p_key=None, research_id=None, attribute=None, lang=None, value=None,
                           create_time=datetime.datetime.now()):
    sql = f"insert into TB_Research_Attribute(uuid,research_id,value,lang,attribute,create_time)values(%s,%s,%s,%s,%s,%s)"
    return execute(sql, (p_key, research_id, value, lang, attribute, create_time))


def get_research_need_parse(start_time):







    sql = """
    SELECT * from TB_Research where create_time>%s and 
    (parse_status='pending' or parse_status is NULL) and download_status='1' order by create_time
    """
    data = query_dict(sql, (start_time,))
    return data


def get_research_need_parse2(start_time):
    sql = """
    SELECT * from TB_Research where create_time>%s and 
    (parse_status='pending' or parse_status is NULL or parse_status = 'parse_fail' or parse_status = 'parsing') and download_status='1' order by create_time
    """
    data = query_dict(sql, (start_time,))
    return data



def get_research_need_preprocess(start_time):
    sql = """
    select * from TB_Research where create_time>%s and file_type='pdf' and embedding_status='embedding_ok' and
    (preprocess_status is null or preprocess_status='pending') order by create_time
    """
    data = query_dict(sql, (start_time,))
    return data


def set_research_attr(research_id, attr, value):
    sql = f"update TB_Research set {attr}='{value}' where uuid='{research_id}'"
    return execute(sql)


def get_research_need_embedding2(start_time):
    sql = """
    select * from TB_Research where create_time>%s and (embedding_status is null or embedding_status='pending' or embedding_status='embedding_fail') and parse_status='parse_ok' order by create_time
    """
    data = query_dict(sql, (start_time,))
    return data

def get_research_need_embedding(start_time):
    sql = """
    select * from TB_Research where create_time>%s and (embedding_status is null or embedding_status='pending') and parse_status='parse_ok' order by create_time
    """
    data = query_dict(sql, (start_time,))
    return data


def get_fargo_insight_research_not_sync(create_time):
    sql = """
    SELECT * from TB_Research where create_time>%s ORDER BY create_time ASC
    """
    data = query_dict(sql, (create_time,))
    return data


def get_research_need_handle_meta(create_time):
    sql = """
       SELECT * from TB_Research where create_time>%s and tag is null ORDER BY create_time ASC limit 100
    """
    data = query_dict(sql, (create_time,))
    return data


def set_research_meta_data(research_id,
                           author,
                           region,
                           company_cn,
                           company_en,
                           stock_ticker,
                           sector,
                           asset_class,
                           tag,
                           industry):
    sql = """
    update TB_Research set 
    author=%s,region=%s,company_cn=%s,company_en=%s,stock_ticker=%s,sector=%s,asset_class=%s,tag=%s,industry=%s
    where uuid=%s
    """
    return execute(sql, (author, region, company_cn, company_en, stock_ticker,
                         sector, asset_class, tag, industry, research_id))
