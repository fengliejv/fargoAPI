import datetime

from pymysql.converters import escape_string

from api.v1.lib.common.mysqlsingle import query_dict, execute
from api.v1.lib.common.utils import add_single_quotes_to_patterns


def get_research(page_size, page_count, search):
    filter = f"and {escape_string(search)} " if search else ""
    filter = add_single_quotes_to_patterns(filter)
    sql = f"SELECT uuid as file_id,publish_time,source,title,tags,author,file_type,lang,source_url,meta_data,local_path from " \
          f"TB_Research where 1=1 {filter} ORDER BY publish_time desc LIMIT %s OFFSET %s;"
    return query_dict(sql, (page_size, (page_size - 1) * page_count))


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

