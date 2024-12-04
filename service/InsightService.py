import uuid

from lib.Common.mysqlsingle import query_dict, execute


def get_report_file_basic_not_sync(latest_time, source='gs'):
    sql = f"select * from TB_File_Basic where create_time>'{latest_time}' and source='{source}' order by create_time asc limit 100"
    data = query_dict(sql)
    return data


def get_fargo_insight_file_basic_not_sync(create_time, source='gs'):
    sql = f"SELECT article_id,ANY_VALUE(local_save_path) AS local_save_path,ANY_VALUE(create_time) AS create_time," \
          f"ANY_VALUE(SOURCE) AS source,ANY_VALUE(original_url) AS original_url,ANY_VALUE(title) AS title," \
          f"ANY_VALUE(UUID) AS uuid,ANY_VALUE(publish_time) AS publish_time,group_concat(symbol Separator ',') as symbol " \
          f"from TB_File_Basic group by article_id HAVING source='{source}' AND create_time>'{create_time}' ORDER BY create_time ASC"
    data = query_dict(sql)
    return data


def get_report_file_basic_by_publish_time(publish_time, source='gs'):
    sql = f"select * from TB_File_Basic where publish_time>'{publish_time}' and source='{source}' order by create_time asc"
    data = query_dict(sql)
    return data


def set_sync_node_time(update_time, node_time, task_name):
    sql = f"update TB_API_Data_Sync_Node set update_time='{update_time}',node_time='{node_time}' where consumer='fargoinsight' and task_name='{task_name}'"
    return execute(sql)


def get_sync_node_time(task_name):
    sql = f"select * from TB_API_Data_Sync_Node where task_name='{task_name}' and consumer='fargoinsight'"
    data = query_dict(sql)
    return data


def add_sync_record(file_id):
    sql = f"insert into TB_FargoInsight_Report_Sync(uuid,article_id) values ('{uuid.uuid1()}','{file_id}')"
    return execute(sql)


def get_sync_record(file_id):
    sql = f"select * from TB_FargoInsight_Report_Sync where article_id='{file_id}'"
    data = query_dict(sql)
    return data
