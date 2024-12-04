from api.v1.lib.common.mysqlsingle import query_dict, execute


def get_file_basic_need_parse(start_time, source):
    sql = f"select uuid,article_id,source,title,local_save_path,create_time,parse_count from TB_File_Basic where create_time>'{start_time}' " \
          f"and (parse_status='pending' or parse_status='parse_fail' or parse_status is null) and source='{source}' order by create_time"
    data = query_dict(sql)
    return data


def get_file_basic_need_embedding(start_time):
    sql = f"select a.uuid,a.source,a.local_save_path,a.create_time,b.version from TB_File_Basic as a inner join " \
          f"TB_File_Parsing as b on a.uuid=b.file_id where a.create_time>'{start_time}' " \
          f"and (embedding_status='pending' or embedding_status='embedding_fail' or embedding_status is null) order by a.create_time"
    data = query_dict(sql)
    return data


def set_file_basic_attr(uuid, attr, value):
    sql = f"update TB_File_Basic set {attr}='{value}' where uuid='{uuid}'"
    if attr == 'parse_status' and value == 'parsing':
        sql = f"update TB_File_Basic set {attr}='{value}',parse_count=parse_count+1 where uuid='{uuid}'"
    if attr == 'embedding_status' and value == 'embedding':
        sql = f"update TB_File_Basic set {attr}='{value}',embedding_count=embedding_count+1 where uuid='{uuid}'"
    return execute(sql)


def check_file_basic_same_file_parsed(article_id, source, start_time):
    sql = f"select uuid from TB_File_Basic where create_time>'{start_time}' and uuid in " \
          f"(select uuid from TB_File_Basic where create_time>'{start_time}' and parse_status='parse_ok' and source='{source}' and article_id='{article_id}') limit 1"
    data = query_dict(sql)
    return data


def get_same_article_info(article_id, source):
    sql = f"select info from TB_File_Basic where file_id in (select uuid from TB_File_Basic where " \
          f"article_id='{article_id}' and source='{source}') limit 1"
    data = query_dict(sql)
    return data


def set_article_info(article_id, source, info):
    sql = f"update TB_File_Basic set info=%s where article_id=%s and source=%s"
    return execute(sql, (info, article_id, source))

