from pymysql.converters import escape_string

from lib.mysql import query_dict, execute
from lib.utils import add_single_quotes_to_patterns


def get_research_by_id(file_id):
    sql = f"select * from TB_Research where uuid=%s limit 1"
    return query_dict(sql, (file_id,))


def get_research(page_size, page_count, search):
    filter = f"and {escape_string(search)} " if search else ""
    filter = add_single_quotes_to_patterns(filter)
    sql = f"SELECT * from TB_Research where 1=1 {filter} ORDER BY publish_time desc LIMIT %s OFFSET %s;"
    return query_dict(sql, (page_size, (page_size - 1) * page_count))


def set_research_attr(research_id, attr, value):
    sql = f"update TB_Research set {attr}='{value}' where uuid='{research_id}'"
    return execute(sql)


def get_research_brief(files_id):
    placeholders = ', '.join(['%s'] * len(files_id))
    sql = f"SELECT * FROM TB_Research WHERE uuid IN ({placeholders})"
    return query_dict(sql, files_id)
