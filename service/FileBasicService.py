from lib.Common.mysqlsingle import query_dict, execute


def get_file_basic_need_parse(start_time, source):
    sql = """
    select uuid,article_id,source,title,local_save_path,create_time,parse_count,file_id from TB_File_Basic 
    where create_time>%s and type='pdf' and (parse_status='pending' or parse_status='parse_fail' or parse_status is null) 
    and source=%s order by create_time
    """
    data = query_dict(sql, (start_time, source))
    return data


def get_file_basic_need_embedding(start_time):
    sql = f"select publish_time,embedding_count,a.article_id,a.uuid,a.source,a.local_save_path,a.create_time,b.version " \
          f"from TB_File_Basic as a inner join TB_File_Parsing as b on a.uuid=b.file_id where " \
          f"a.create_time>'{start_time}' and type='pdf' and (source='ms' or source='ubs' or source='gs' or source='quartr_report') " \
          f"and (embedding_status='pending' or embedding_status='embedding_fail' or embedding_status is null) order by a.create_time"
    data = query_dict(sql)
    return data


def get_file_basic_quartr_embedding(start_time):
    sql = f"select embedding_count,article_id,uuid,source,local_save_path,create_time,publish_time from TB_File_Basic " \
          f"where create_time>'{start_time}' and type='json' and source='quartr_ts' and (embedding_status='pending' " \
          f"or embedding_status='embedding_fail' or embedding_status is null) order by create_time"
    data = query_dict(sql)
    return data


def set_file_basic_attr(uuid, attr, value):
    sql = f"update TB_File_Basic set {attr}='{value}' where uuid='{uuid}'"
    if attr == 'parse_status' and value == 'parsing':
        sql = f"update TB_File_Basic set {attr}='{value}',parse_count=parse_count+1 where uuid='{uuid}'"
    if attr == 'embedding_status' and value == 'embedding':
        sql = f"update TB_File_Basic set {attr}='{value}',embedding_count=embedding_count+1 where uuid='{uuid}'"
    return execute(sql)


def set_same_file_embedding_status(article_id, source):
    sql = f"update TB_File_Basic set embedding_status='embedding_ok' where article_id='{article_id}' and source='{source}'"
    return execute(sql)


def check_file_basic_same_file_parsed(article_id, source, start_time):
    sql = f"select uuid from TB_File_Basic where create_time>'{start_time}' and uuid in " \
          f"(select uuid from TB_File_Basic where create_time>'{start_time}' and parse_status='parse_ok' and source='{source}' and article_id='{article_id}') limit 1"
    data = query_dict(sql)
    return data
