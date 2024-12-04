import datetime
import io
import uuid

from lib.Common.my_minio import Bucket
from lib.Common.mysqlsingle import query_dict, execute


def get_galpha_file_basic_not_parsing(create_time, source='gs'):
    sql = f"SELECT article_id,ANY_VALUE(local_save_path) AS local_save_path,ANY_VALUE(create_time) AS create_time," \
          f"ANY_VALUE(SOURCE) AS source,ANY_VALUE(original_url) AS original_url,ANY_VALUE(title) AS title," \
          f"ANY_VALUE(UUID) AS uuid,ANY_VALUE(publish_time) AS publish_time,group_concat(symbol Separator ',') as symbol " \
          f"from TB_File_Basic group by article_id HAVING source='{source}' AND create_time>'{create_time}' ORDER BY create_time ASC"
    data = query_dict(sql)
    return data


def get_galpha_file_basic_path():
    sql = f"select * from TB_File_Basic where local_save_path='/home/ibagents/files/ubs/PDDHoldings/uep94525.pdf' LIMIT 1"
    data = query_dict(sql)
    return data


def get_report_file_basic_by_publish_time(publish_time, source='gs'):
    sql = f"select * from TB_File_Basic where publish_time>'{publish_time}' and source='{source}' order by create_time asc"
    data = query_dict(sql)
    return data


def get_all_not_upload():
    sql = f"SELECT a.file_id,b.local_save_path,a.result,a.version FROM (TB_File_Parsing AS a INNER JOIN TB_File_Basic AS b ON a.file_id=b.uuid) WHERE a.upload_status=0"
    data = query_dict(sql)
    return data


def set_sync_node_time(update_time, node_time, task_name):
    sql = f"update TB_API_Data_Sync_Node set update_time='{update_time}',node_time='{node_time}' where consumer='galpha' and task_name='{task_name}'"
    return execute(sql)


def set_parsed_file_status(file_id):
    sql = f"update TB_File_Parsing set upload_status=1 where file_id='{file_id}'"
    return execute(sql)


def get_sync_node_time(task_name):
    sql = f"select * from TB_API_Data_Sync_Node where task_name='{task_name}' and consumer='galpha'"
    data = query_dict(sql)
    return data


def add_sync_record(file_id):
    sql = f"insert into TB_FargoInsight_Report_Sync(uuid,article_id) values ('{uuid.uuid1()}','{file_id}')"
    return execute(sql)


def get_consumer_node_time(task_name, consumer):
    sql = f"select * from TB_API_Data_Sync_Node where task_name='{task_name}' and consumer='{consumer}'"
    data = query_dict(sql)
    return data


def get_parsing_record(article_id, version_id):
    sql = f"select * from TB_File_Parsing where article_id='{article_id}' and version='{version_id}'"
    data = query_dict(sql)
    return data


def add_parsing_record(file_id, parsing_platform, req, result, article_id, response, version):
    minio_obj = Bucket()
    add_res = True
    if minio_obj.exists_bucket("report-parse-result"):
        value_as_bytes = result.encode('utf-8')
        minio_obj.upload_bytes(bucket_name="report-parse-result", key=f"{file_id}_{version}",
                               bytes_content=value_as_bytes)
    if minio_obj.exists_bucket("report-parse-response"):
        value_as_bytes = response.encode('utf-8')
        up_res = minio_obj.upload_bytes(bucket_name="report-parse-response", key=f"{file_id}_{version}",
                                        bytes_content=value_as_bytes)
        if not up_res:
            add_res = False
    del minio_obj
    sql = f"insert into TB_File_Parsing(uuid,file_id,parsing_platform,create_time,req,result,article_id,version,upload_status) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    count = execute(sql, (
        f"{uuid.uuid1()}", file_id, parsing_platform, datetime.datetime.now(), req, "", article_id,
        version, False))
    if not count:
        add_res = False
    return add_res


def add_parsed_record(file_id, parsing_platform, req, article_id, version):
    sql = f"insert into TB_File_Parsing(uuid,file_id,parsing_platform,create_time,req,result,article_id,version,upload_status) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    count = execute(sql, (
        f"{uuid.uuid1()}", file_id, parsing_platform, datetime.datetime.now(), req, "", article_id,
        version, False))
    return count
